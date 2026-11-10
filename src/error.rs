use crate::util::ProtocolMessage;
use solana_client::client_error::ClientError;
use solana_program::pubkey::ParsePubkeyError;
use std::array::TryFromSliceError;
use std::fmt::{Debug, Display, Formatter};
use std::io::Error;
use tokio::sync::mpsc::error::SendError;
pub enum AggError {
    ClientError(ClientError),
    UnableToParsePublicKey(ParsePubkeyError),
    ConversionError(TryFromSliceError),
    MpscChannelError(SendError<ProtocolMessage>),
    OneshotChannelError,
    DbError(rocksdb::Error),
    JsonError(serde_json::Error),
    ServerError(std::io::Error),
    BlockNotFound,
    NoBlockFinalised,
    TxNotFound,
}

impl Display for AggError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let err_mgs = match self {
            AggError::ClientError(err) => format_client_error(err, false),
            AggError::UnableToParsePublicKey(err) => format!("Unable to parse public key: {}", err),
            AggError::ConversionError(err) => format!("Conversion Error: {}", err),
            AggError::MpscChannelError(err) => format!("Mpsc Channel Error: {}", err),
            AggError::OneshotChannelError => "Oneshot Channel Error".to_string(),
            AggError::DbError(err) => format!("Db Error: {}", err),
            AggError::JsonError(err) => format!("Json Error: {}", err),
            AggError::BlockNotFound => "Block Not Found".to_string(),
            AggError::NoBlockFinalised => "No Block Finalised".to_string(),
            AggError::TxNotFound => "Transaction Not Found".to_string(),
            AggError::ServerError(err) => format!("Server Error {}", err),
        };
        write!(f, "{}", err_mgs)
    }
}

impl Debug for AggError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let err_mgs = match self {
            AggError::ClientError(err) => format_client_error(err, true),
            AggError::UnableToParsePublicKey(err) => {
                format!("Unable to parse public key: {:?}", err)
            }
            AggError::ConversionError(err) => format!("Conversion Error: {:?}", err),
            AggError::MpscChannelError(err) => format!("Mpsc Channel Error: {:?}", err),
            AggError::OneshotChannelError => "Oneshot Channel Error".to_string(),
            AggError::DbError(err) => format!("Db Error: {:?}", err),
            AggError::JsonError(err) => format!("Json Error: {:?}", err),
            AggError::BlockNotFound => "Block Not Found".to_string(),
            AggError::NoBlockFinalised => "No Block Finalised".to_string(),
            AggError::TxNotFound => "Transaction Not Found".to_string(),
            AggError::ServerError(err) => format!("Server Error {:?}", err),
        };
        write!(f, "{}", err_mgs)
    }
}

impl From<ClientError> for AggError {
    fn from(err: ClientError) -> Self {
        Self::ClientError(err)
    }
}

impl From<ParsePubkeyError> for AggError {
    fn from(err: ParsePubkeyError) -> Self {
        Self::UnableToParsePublicKey(err)
    }
}

impl From<TryFromSliceError> for AggError {
    fn from(err: TryFromSliceError) -> Self {
        Self::ConversionError(err)
    }
}

impl From<SendError<ProtocolMessage>> for AggError {
    fn from(err: SendError<ProtocolMessage>) -> Self {
        Self::MpscChannelError(err)
    }
}

impl From<rocksdb::Error> for AggError {
    fn from(err: rocksdb::Error) -> Self {
        Self::DbError(err)
    }
}

impl From<serde_json::Error> for AggError {
    fn from(err: serde_json::Error) -> Self {
        Self::JsonError(err)
    }
}

impl From<std::io::Error> for AggError {
    fn from(value: Error) -> Self {
        Self::ServerError(value)
    }
}

fn format_client_error(err: &ClientError, debug: bool) -> String {
    let rendered = if debug {
        format!("{err:?}")
    } else {
        err.to_string()
    };
    format!("Client Error: {}", redact_rpc_secrets(&rendered))
}

// solana-client puts the RPC URL in ClientError's Display and Debug output.
fn redact_rpc_secrets(input: &str) -> String {
    redact_named_secrets(&redact_debug_secret_fields(&redact_http_urls(input)))
}

fn redact_http_urls(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = find_http_scheme(rest) {
        output.push_str(&rest[..start]);
        let url_len = http_url_len(&rest[start..]).unwrap_or(rest[start..].len());
        output.push_str(&redact_one_url(&rest[start..start + url_len]));
        rest = &rest[start + url_len..];
    }
    output.push_str(rest);
    output
}

fn find_http_scheme(input: &str) -> Option<usize> {
    let mut offset = 0;
    while offset < input.len() {
        if is_boundary(input, offset) && scheme_prefix_len(&input[offset..]).is_some() {
            return Some(offset);
        }
        offset += next_char_len(input, offset);
    }
    None
}

fn http_url_len(input: &str) -> Option<usize> {
    let scheme_len = scheme_prefix_len(input)?;
    let rest = &input[scheme_len..];
    let end = rest.find(is_url_terminator).unwrap_or(rest.len());
    Some(scheme_len + end)
}

fn scheme_prefix_len(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    if bytes.len() >= 8 && bytes[..8].eq_ignore_ascii_case(b"https://") {
        Some(8)
    } else if bytes.len() >= 7 && bytes[..7].eq_ignore_ascii_case(b"http://") {
        Some(7)
    } else {
        None
    }
}

fn redact_one_url(url: &str) -> String {
    let Some(scheme_len) = scheme_prefix_len(url) else {
        return url.to_string();
    };
    let rest = &url[scheme_len..];
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let after_authority = &rest[authority_end..];

    let mut redacted = String::with_capacity(url.len());
    redacted.push_str(&url[..scheme_len]);
    if let Some(at) = authority.rfind('@') {
        redacted.push_str("REDACTED@");
        redacted.push_str(&authority[at + 1..]);
    } else {
        redacted.push_str(authority);
    }

    if let Some(suffix_at) = after_authority.find(['?', '#']) {
        redacted.push_str(&after_authority[..suffix_at]);
        redacted.push_str("?REDACTED");
    } else {
        redacted.push_str(after_authority);
    }
    redacted
}

fn redact_debug_secret_fields(input: &str) -> String {
    const PREFIXES: &[&str] = &[
        "query: Some(",
        "password: Some(",
        "fragment: Some(",
        "username: ",
    ];

    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    while let Some((index, prefix)) = earliest_prefix(rest, PREFIXES) {
        output.push_str(&rest[..index]);
        let after_prefix = &rest[index + prefix.len()..];
        if let Some(value_len) = quoted_value_len(after_prefix) {
            let quoted = &after_prefix[..value_len];
            let inner_start = quoted.find('"').unwrap_or(0) + 1;
            let inner = &quoted[inner_start..quoted.len() - 1];
            if inner.is_empty() {
                output.push_str(&rest[index..index + prefix.len() + value_len]);
            } else {
                output.push_str(prefix);
                output.push_str("\"REDACTED\"");
            }
            rest = &after_prefix[value_len..];
            continue;
        }
        output.push_str(prefix);
        rest = after_prefix;
    }
    output.push_str(rest);
    output
}

fn redact_named_secrets(input: &str) -> String {
    const NAMES: &[&str] = &["api-key", "api_key", "apikey"];
    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    while let Some((index, name_and_sep)) = earliest_secret_param(rest, NAMES) {
        output.push_str(&rest[..index]);
        output.push_str(&rest[index..index + name_and_sep]);
        let value = &rest[index + name_and_sep..];
        let value_len = value.find(is_param_value_terminator).unwrap_or(value.len());
        output.push_str("REDACTED");
        rest = &value[value_len..];
    }
    output.push_str(rest);
    output
}

fn earliest_prefix<'a>(input: &str, prefixes: &'a [&'a str]) -> Option<(usize, &'a str)> {
    let mut best: Option<(usize, &'a str)> = None;
    for prefix in prefixes {
        if let Some(index) = find_at_boundary(input, prefix) {
            let replace = match best {
                Some((best_index, _)) => index < best_index,
                None => true,
            };
            if replace {
                best = Some((index, *prefix));
            }
        }
    }
    best
}

fn earliest_secret_param(input: &str, names: &[&str]) -> Option<(usize, usize)> {
    let mut offset = 0;
    while offset < input.len() {
        if is_boundary(input, offset) {
            if let Some(name_and_sep) = secret_param_len(&input[offset..], names) {
                return Some((offset, name_and_sep));
            }
        }
        offset += next_char_len(input, offset);
    }
    None
}

fn secret_param_len(input: &str, names: &[&str]) -> Option<usize> {
    let bytes = input.as_bytes();
    for name in names {
        if bytes.len() < name.len() || !bytes[..name.len()].eq_ignore_ascii_case(name.as_bytes()) {
            continue;
        }
        let rest = &input[name.len()..];
        if rest.starts_with('=') {
            return Some(name.len() + 1);
        }
        if rest.len() >= 3 && rest.as_bytes()[..3].eq_ignore_ascii_case(b"%3d") {
            return Some(name.len() + 3);
        }
    }
    None
}

fn quoted_value_len(input: &str) -> Option<usize> {
    let trimmed = input.trim_start_matches(|c: char| matches!(c, ' ' | '\n' | '\r' | '\t'));
    let whitespace = input.len() - trimmed.len();
    if !trimmed.starts_with('"') {
        return None;
    }
    let close = escaped_quote_end(&trimmed[1..])?;
    Some(whitespace + 1 + close + 1)
}

fn escaped_quote_end(input: &str) -> Option<usize> {
    let mut escaped = false;
    for (index, ch) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '"' {
            return Some(index);
        }
    }
    None
}

fn find_at_boundary(input: &str, pattern: &str) -> Option<usize> {
    let mut search_from = 0;
    while let Some(relative) = input[search_from..].find(pattern) {
        let index = search_from + relative;
        if is_boundary(input, index) {
            return Some(index);
        }
        search_from = index + pattern.len().max(1);
    }
    None
}

fn is_boundary(input: &str, index: usize) -> bool {
    if index == 0 {
        return true;
    }
    !input[..index]
        .chars()
        .next_back()
        .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn is_url_terminator(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, '"' | '\'' | '<' | '>' | ')')
}

fn is_param_value_terminator(ch: char) -> bool {
    ch.is_whitespace()
        || matches!(
            ch,
            '&' | '#' | '"' | '\'' | ')' | ',' | '}' | ']' | '<' | '>' | '\\'
        )
}

fn next_char_len(input: &str, index: usize) -> usize {
    input[index..]
        .chars()
        .next()
        .map(char::len_utf8)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::{redact_rpc_secrets, AggError};
    use solana_client::rpc_client::RpcClient;
    use std::time::Duration;

    const SECRET: &str = "synthetic-test-credential";

    #[test]
    fn redactor_strips_query_userinfo_and_debug_fields() {
        let display = format!(
            "error sending request for url (http://127.0.0.1:9/?api-key={SECRET}): connection refused"
        );
        let redacted_display = redact_rpc_secrets(&display);
        assert!(!redacted_display.contains(SECRET));
        assert!(redacted_display.contains("127.0.0.1"));
        assert!(redacted_display.contains("connection refused"));

        let debug = format!(
            r#"Url {{ username: "", password: None, query: Some("api-key={SECRET}"), fragment: None }}"#
        );
        let redacted_debug = redact_rpc_secrets(&debug);
        assert!(!redacted_debug.contains(SECRET));
        assert!(redacted_debug.contains("password: None"));

        let userinfo = format!("https://rpc-user:{SECRET}@example.com/rpc");
        let redacted_userinfo = redact_rpc_secrets(&userinfo);
        assert!(!redacted_userinfo.contains(SECRET));
        assert!(redacted_userinfo.contains("example.com/rpc"));

        let encoded = format!("https://example.com/rpc?api_key%3D{SECRET}&x=1");
        assert!(!redact_rpc_secrets(&encoded).contains(SECRET));
        assert_eq!(redact_rpc_secrets("Block Not Found"), "Block Not Found");
    }

    #[test]
    fn synthetic_rpc_credential_is_hidden_in_application_errors() {
        let url = format!("http://127.0.0.1:1/?api-key={SECRET}");
        let client = RpcClient::new_with_timeout(url, Duration::from_secs(2));
        let err = client
            .get_slot()
            .expect_err("RPC call to a closed local port should fail");

        let raw_display = err.to_string();
        let raw_debug = format!("{err:?}");
        assert!(
            raw_display.contains(SECRET) || raw_debug.contains(SECRET),
            "solana-client did not include the synthetic credential, so redaction was not exercised against dependency output.\n display: {raw_display}\n debug: {raw_debug}"
        );

        let agg = AggError::from(err);
        let outputs = [
            agg.to_string(),
            format!("{agg:?}"),
            format!("Error from subscriber client {agg}"),
            format!("Failed to fetch latest slot {agg:?}"),
            format!("Failed to fetch block {agg:?}"),
            format!("Error from db client {agg}"),
        ];
        for output in outputs {
            assert!(
                !output.contains(SECRET),
                "application error output revealed the RPC credential: {output}"
            );
            assert!(
                output.contains("Client Error:"),
                "application error output dropped the client error context: {output}"
            );
            assert!(
                output.contains("127.0.0.1"),
                "application error output dropped the RPC host: {output}"
            );
        }
    }
}
