use serde::Serialize;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

pub const SHUTDOWN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServicePhase {
    Starting,
    Indexing,
    CaughtUp,
    Stopping,
    Failed,
}

impl ServicePhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Indexing => "indexing",
            Self::CaughtUp => "caught_up",
            Self::Stopping => "stopping",
            Self::Failed => "failed",
        }
    }

    pub fn is_healthy(self) -> bool {
        matches!(self, Self::Starting | Self::Indexing | Self::CaughtUp)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeStatus {
    pub phase: ServicePhase,
    pub last_processed_slot: Option<u64>,
    pub last_indexed_block_height: Option<u64>,
    pub last_observed_finalized_slot: Option<u64>,
    pub last_successful_processing_time: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Termination {
    Shutdown,
    Failed(String),
}

impl Termination {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Shutdown => 0,
            Self::Failed(_) => 1,
        }
    }
}

pub struct Lifecycle {
    status: Mutex<RuntimeStatus>,
    shutting_down: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
}

impl Lifecycle {
    pub fn new() -> (Arc<Self>, watch::Receiver<bool>) {
        let (shutdown, receiver) = watch::channel(false);
        let lifecycle = Arc::new(Self {
            status: Mutex::new(RuntimeStatus {
                phase: ServicePhase::Starting,
                last_processed_slot: None,
                last_indexed_block_height: None,
                last_observed_finalized_slot: None,
                last_successful_processing_time: None,
                error: None,
            }),
            shutting_down: Arc::new(AtomicBool::new(false)),
            shutdown,
        });
        (lifecycle, receiver)
    }

    pub fn stopping_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutting_down)
    }

    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        let mut status = self.status.lock().expect("lifecycle");
        if status.phase != ServicePhase::Failed {
            status.phase = ServicePhase::Stopping;
        }
        let _ = self.shutdown.send(true);
    }

    pub fn is_stopping(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    pub fn is_healthy(&self) -> bool {
        self.status.lock().expect("lifecycle").phase.is_healthy()
    }

    pub fn snapshot(&self) -> RuntimeStatus {
        self.status.lock().expect("lifecycle").clone()
    }

    pub fn set_phase(&self, phase: ServicePhase) {
        let mut status = self.status.lock().expect("lifecycle");
        if status.phase == ServicePhase::Stopping || status.phase == ServicePhase::Failed {
            return;
        }
        status.phase = phase;
    }

    pub fn note_checkpoint(&self, slot: u64) {
        self.status.lock().expect("lifecycle").last_processed_slot = Some(slot);
    }

    pub fn note_block_height(&self, height: u64) {
        self.status
            .lock()
            .expect("lifecycle")
            .last_indexed_block_height = Some(height);
    }

    pub fn note_finalized_tip(&self, slot: u64) {
        self.status
            .lock()
            .expect("lifecycle")
            .last_observed_finalized_slot = Some(slot);
    }

    pub fn note_success(&self) {
        self.status
            .lock()
            .expect("lifecycle")
            .last_successful_processing_time = Some(unix_millis());
    }

    pub fn fail(&self, message: String) {
        let mut status = self.status.lock().expect("lifecycle");
        status.phase = ServicePhase::Failed;
        status.error = Some(message);
        self.shutting_down.store(true, Ordering::SeqCst);
        let _ = self.shutdown.send(true);
    }
}

pub fn termination_after_component(
    lifecycle: &Lifecycle,
    component: &str,
    panicked: bool,
) -> Termination {
    if panicked {
        let message = format!("{component} panicked");
        lifecycle.fail(message.clone());
        return Termination::Failed(message);
    }
    match unexpected_component_stop(lifecycle.is_stopping(), component) {
        Some(message) => {
            lifecycle.fail(message.clone());
            Termination::Failed(message)
        }
        None => Termination::Shutdown,
    }
}

pub async fn finish_within<T>(deadline: Duration, work: impl Future<Output = T>) -> Result<T, ()> {
    match tokio::time::timeout(deadline, work).await {
        Ok(value) => Ok(value),
        Err(_) => Err(()),
    }
}

pub fn unexpected_component_stop(shutting_down: bool, component: &str) -> Option<String> {
    if shutting_down {
        None
    } else {
        Some(format!("{component} stopped unexpectedly"))
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Serialize)]
pub struct HealthBody {
    pub status: &'static str,
}

#[derive(Serialize)]
pub struct StatusBody {
    pub state: &'static str,
    pub last_processed_slot: Option<u64>,
    pub last_indexed_block_height: Option<u64>,
    pub last_observed_finalized_slot: Option<u64>,
    pub last_successful_processing_time: Option<u64>,
    pub error: Option<String>,
}

impl From<RuntimeStatus> for StatusBody {
    fn from(status: RuntimeStatus) -> Self {
        Self {
            state: status.phase.as_str(),
            last_processed_slot: status.last_processed_slot,
            last_indexed_block_height: status.last_indexed_block_height,
            last_observed_finalized_slot: status.last_observed_finalized_slot,
            last_successful_processing_time: status.last_successful_processing_time,
            error: status.error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        finish_within, termination_after_component, unexpected_component_stop, Lifecycle,
        ServicePhase, Termination,
    };

    #[test]
    fn intentional_shutdown_exits_zero_and_failure_exits_nonzero() {
        assert_eq!(Termination::Shutdown.exit_code(), 0);
        assert_eq!(Termination::Failed("handler stopped".into()).exit_code(), 1);
    }

    #[test]
    fn fresh_status_does_not_claim_an_indexed_block() {
        let (lifecycle, _shutdown) = Lifecycle::new();
        let status = lifecycle.snapshot();
        assert_eq!(status.phase, ServicePhase::Starting);
        assert_eq!(status.last_indexed_block_height, None);
        assert!(lifecycle.is_healthy());
        lifecycle.note_checkpoint(80);
        let status = lifecycle.snapshot();
        assert_eq!(status.last_processed_slot, Some(80));
        assert_eq!(status.last_indexed_block_height, None);
    }

    #[test]
    fn skipped_slot_does_not_invent_a_block_height() {
        let (lifecycle, _shutdown) = Lifecycle::new();
        lifecycle.note_checkpoint(11);
        lifecycle.note_success();
        let status = lifecycle.snapshot();
        assert_eq!(status.last_processed_slot, Some(11));
        assert_eq!(status.last_indexed_block_height, None);
        assert!(status.last_successful_processing_time.is_some());
    }

    #[test]
    fn failure_is_unhealthy_and_unexpected_stop_is_fatal_only_while_running() {
        let (lifecycle, _shutdown) = Lifecycle::new();
        assert_eq!(
            unexpected_component_stop(false, "database worker").as_deref(),
            Some("database worker stopped unexpectedly")
        );
        lifecycle.fail("block processing failed".into());
        assert!(!lifecycle.is_healthy());
        assert!(lifecycle.is_stopping());
        assert_eq!(
            lifecycle.snapshot().error.as_deref(),
            Some("block processing failed")
        );
        assert_eq!(unexpected_component_stop(true, "handler"), None);
    }

    #[test]
    fn subscriber_handler_and_database_failures_start_shutdown() {
        for component in ["subscriber", "handler", "database worker"] {
            let (lifecycle, _shutdown) = Lifecycle::new();
            let termination = termination_after_component(&lifecycle, component, false);
            assert_eq!(termination.exit_code(), 1);
            assert!(lifecycle.is_stopping());
            assert!(!lifecycle.is_healthy());
            assert_eq!(
                lifecycle.snapshot().error.as_deref(),
                Some(format!("{component} stopped unexpectedly").as_str())
            );
        }
    }

    #[test]
    fn shutdown_already_in_progress_is_not_a_second_failure() {
        let (lifecycle, _shutdown) = Lifecycle::new();
        lifecycle.begin_shutdown();
        let termination = termination_after_component(&lifecycle, "handler", false);
        assert_eq!(termination, Termination::Shutdown);
        assert_eq!(termination.exit_code(), 0);
        assert!(lifecycle.snapshot().error.is_none());
    }

    #[test]
    fn panicked_worker_fails_even_during_shutdown() {
        let (lifecycle, _shutdown) = Lifecycle::new();
        lifecycle.begin_shutdown();
        let termination = termination_after_component(&lifecycle, "database worker", true);
        assert_eq!(termination.exit_code(), 1);
        assert_eq!(
            lifecycle.snapshot().error.as_deref(),
            Some("database worker panicked")
        );
    }

    #[tokio::test]
    async fn deadline_reports_unresolved_work_without_waiting_forever() {
        let pending = std::future::pending::<()>();
        tokio::pin!(pending);
        let unresolved = finish_within(std::time::Duration::from_millis(20), &mut pending).await;
        assert!(unresolved.is_err());
        let ready = finish_within(std::time::Duration::from_secs(1), async { 7 }).await;
        assert_eq!(ready, Ok(7));
    }
}
