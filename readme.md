# RuSQL

A minimal, from-scratch database engine in Rust.

## Highlights
- B+Tree based key-value storage
- Copy-on-write architecture
- Memory-mapped file I/O 
- Free-list garbage collection
- Crash resilience through rollbacks
- Type-safe error handling 
- Supports tables and secondary indices
- Multi threaded transactions
- Read optimized through shared LRU cache
