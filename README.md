A lightweight, in-memory Redis-clone key-value storage written in Go. Supports atomic operations, TTLs, and concurrency-safe access.

## Features

- **Core Commands**
  - `SET` – store a key/value with optional NX/XX and TTL (EX/PX)
  - `GET` – retrieve a key, respecting TTL
  - `DEL` – delete a key
  - `EXPIRE` – set TTL in seconds with optional NX/XX
  - `TTL` – get remaining TTL in seconds
  - `PTTL` – get remaining TTL in milliseconds
  - `PERSIST` – remove TTL from a key
  - `INCR` – atomically increment an integer value
  - `DECR` – atomically decrement an integer value
  - `INCRBY` – atomically increment an integer value by a given amount
  - `INCRBY` – atomically decrements an integer value by a given amount
  - `PING` – server liveness check

- **TTL Handling**
  - Supports EX (seconds) and PX (milliseconds)
  - Immediate deletion when TTL ≤ 0
  - Background janitor cleans expired keys
  - Lazy expiration ensures all commands see correct state

- **Concurrency Safe**
  - All operations protected by a mutex
  - Atomic reads and writes

- **RESP Protocol Ready**
  - Designed to integrate with RESP handlers for network communication

- **TODOs**:
 - Having a better logger
 - Seperate package for the Server
 - New Data Types: Sets, Hashes, and JŚON.
 - Sharding
 - Mock DB for tests
