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

## Design Highlights

- **Separation of Concerns**
  - Storage layer manages data and TTLs
  - Protocol/command layer handles parsing and validation

- **Atomic Operations**
  - NX/XX semantics implemented correctly
  - TTL updates handled atomically with value updates

## Usage

```go
store := NewStorage()
store.Set(SetArgs{Key: "foo", Value: "bar", EX: 10})
val, err := store.Get("foo")
store.Expire(ExpireArgs{Key: "foo", Seconds: 5})

