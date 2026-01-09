# RuSQL

A minimal, from-scratch database engine in Rust.

## Highlights
- B+Tree based key-value storage
- Memory-mapped file I/O 
- Free-list garbage collection
- Durable copy-on-write architecture
- Crash resilience through rollbacks
- Type-safe error handling 
- In-memory pager implementation for fast unit testing
- Full table support
- Multi Threaded Transaction support
- Read optimized through shared lock-free cache
