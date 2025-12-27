# RuSQL

A minimal, from-scratch database engine in Rust.

## Highlights
- B+Tree based key-value storage
- Memory-mapped file I/O (rustix-backed)
- Free-list garbage collection
- Durable copy-on-write architecture
- Crash resilience through rollbacks
- Type-safe error handling 
- In-memory pager implementation for fast unit testing
- Full table support
