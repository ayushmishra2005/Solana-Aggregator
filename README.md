## Solana Aggregator Project

This project implements a Solana Aggregator that fetches blocks from a Solana Node, parses them, and aggregates the data into a database. It also provides APIs to retrieve the aggregated data.

### Components

- **Subscriber**: Fetches the latest slot from the Solana Node and triggers the Block Fetcher.
- **Block Fetcher**: Retrieves blocks from the Solana Node, divides them into chunks, and asynchronously invokes the Parser for each chunk.
- **Parser**: Parses a given chunk and sends the parsed chunk to the Handler via a channel.
- **Handler**: Collects all chunks from the channel, orders them, aggregates them into a complete parsed block, and sends it to the DbHandler via a channel.
- **DbHandler**: Collects blocks from the channel, inserts them into the database, and updates the latest block number.
- **Server**: Handles various APIs and fetches data based on the query.

### Sequence Diagram
![solana.png](..%2F..%2F..%2FDownloads%2Fsolana.png)

### Features

- Utilizes **Type State Builders Pattern**.
- Components work asynchronously to match Solana's throughput.
- Uses RocksDB for fast write and query operations.
- Retrieves AccountInfo of any user at any block.
- Simplified error handling.

### Database Design

- **Database**: Uses RocksDB, a NoSQL database, for efficient data insertion and querying.
- **Data Storage**:
    - `[Block No] -> [Block]`
    - `[TxId] -> [Block No]`
    - `[LATEST_BLOCK] -> [Block No]`
- Stores AccountID and total Sol tokens in the latest block.
- Retrieves historical AccountInfo of a user at any given block.

### API Endpoints

- **Get Transaction Details**:
  ```shell
  curl -X GET "http://127.0.0.1:9944/tx_details/{tx_id}" -H "accept: application/json"
  ```
- **Get Latest Block and Details**:
  ```shell
  curl -X GET "http://127.0.0.1:9944/latest_block" -H "accept: application/json"
  ```
- **Get Blocks in Range**:
  ```shell
  curl -X GET "http://127.0.0.1:9944/block_range/{StartBlock}/{EndBlock}" -H "accept: application/json"
  ```
- **Get AccountInfo of User's Public Key**:
  ```shell
  curl -X GET "http://127.0.0.1:9944/account_balance/{PublicKey}" -H "accept: application/json"
  ```
- **Get AccountInfo History at Specific Block**:
  ```shell
  curl -X GET "http://127.0.0.1:9944/account_balance/{PublicKey}/?{BlockNo}" -H "accept: application/json"
  ```

### Future Improvements

- Replace JSON Codec with SCALE or BOSH for more efficient storage.
