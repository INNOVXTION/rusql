# RusQL

A minimal, from-scratch key-value database engine in Rust.

## Implemented features
- B-Tree based key-value storage with node splitting/merging
- Memory-mapped file I/O (rustix-backed)
- Free-list garbage collector for reusing freed pages
- Simple ACID-like metadata checkpointing and rollback
- Type-safe error handling across DB operations
- In-memory pager implementation for fast unit testing
