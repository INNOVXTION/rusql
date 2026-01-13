use std::{
    fmt::{Debug, Display},
    io,
    num::TryFromIntError,
    str::Utf8Error,
};

use crate::database::{tables::TypeCol, transactions::keyrange::Touched};

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

// #[error("{var}")]    ⟶   write!("{}", self.var)
// #[error("{0}")]      ⟶   write!("{}", self.0)
// #[error("{var:?}")]  ⟶   write!("{:?}", self.var)
// #[error("{0:?}")]    ⟶   write!("{:?}", self.0)

#[derive(Debug, Error)]
pub enum Error {
    // tree
    #[error("Index error")]
    IndexError,
    #[error("Error when splitting, {0}")]
    SplitError(String),
    #[error("Error when merging, {0}")]
    MergeError(String),
    #[error("Error when inserting, {0}")]
    InsertError(String),
    #[error("Error when deleting, {0}")]
    DeleteError(String),
    #[error("Search Error, {0}")]
    SearchError(String),

    // wrapper
    #[error("Pager error, {0}")]
    PagerError(#[from] PagerError),
    #[error("Freelist error, {0}")]
    FreeListError(#[from] FLError),
    #[error("Table error, {0}")]
    TableError(#[from] TableError),
    #[error("Scan error, {0}")]
    ScanError(#[from] ScanError),
    #[error("Transaction error, {0}")]
    TransactionError(#[from] TXError),

    // casting
    #[error("Casting from String error, {0}")]
    StrCastError(#[from] Utf8Error),
    #[error("Int casting error, {0}")]
    IntCastError(#[from] TryFromIntError),

    // file I/O
    #[error("File error: {0}")]
    FileError(#[from] io::Error),
    #[error("Sys File Error: {0}")]
    SysFileError(#[from] rustix::io::Errno),
}

#[derive(Debug, Error)]
pub enum PagerError {
    #[error("an unrecovable error occured")]
    UnkownError,
    #[error("Couldnt retrieve page: {0}")]
    PageNotFound(u64),
    #[error("No free pages available")]
    NoAvailablePage,
    #[error("Deallocation failed for page: {0}")]
    DeallocError(u64),
    #[error("Error when encoding/decoding node: {0}")]
    CodecError(#[from] io::Error),
    #[error("Invalid Filename, make sure it doesnt end with / ")]
    FileNameError,
    #[error("Page size but OS is not allowed!")]
    UnsupportedOS,
    #[error("Offset {0} is invalid!")]
    UnalignedOffset(u64),
    #[error("Length {0} is invalid!")]
    UnalignedLength(usize),
    #[error("{0}")]
    PageWriteError(String),

    // syscalls
    #[error("Error when handling file: {0}")]
    FDError(#[from] rustix::io::Errno),
    #[error("Error when calling fsync {0}")]
    FsyncError(rustix::io::Errno),
    #[error("Error when calling mmap {0}")]
    MMapError(rustix::io::Errno),
    #[error("Error when calling pwrite {0}")]
    WriteFileError(rustix::io::Errno),
}

#[derive(Debug, Error)]
pub enum FLError {
    #[error("an unkown error occured")]
    UnknownError,
    #[error("{0}")]
    TruncateError(String),
    #[error("{0}")]
    PopError(String),
}

#[derive(Error, Debug)]
pub(crate) enum TableError {
    // Record
    #[error("invalid Record (expected {expected:?}, found {found:?})")]
    RecordEncodeError { expected: TypeCol, found: String },
    #[error("Record error {0}")]
    RecordError(String),

    // Query
    #[error("invalid Query (expected {expected:?}, found {found:?})")]
    QueryEncodeError { expected: TypeCol, found: String },
    #[error("Query error {0}")]
    QueryError(String),

    // Table
    #[error("Table build error {0}")]
    TableBuildError(String),
    #[error("Insert table error {0}")]
    InsertTableError(String),
    #[error("Get table error {0}")]
    GetTableError(String),
    #[error("Delete table error {0}")]
    DeleteTableError(String),
    #[error("Encode table error {0}")]
    SerializeTableError(serde_json::Error),
    #[error("Encode table error {0}")]
    EncodeTableError(String),
    #[error("Delete table error {0}")]
    DeserializeTableError(serde_json::Error),
    #[error("Table id error {0}")]
    TableIdError(String),

    // Cell
    #[error("Invalid input")]
    CellEncodeError,
    #[error("Error when decoding cell")]
    CellDecodeError,

    // String
    #[error("unknown error...")]
    UnknownError,
    #[error("string format error {0}")]
    StringFormatError(#[from] std::fmt::Error),

    // Key
    #[error("Key encode error {0}")]
    KeyEncodeError(String),
    #[error("Key decode error {0}")]
    KeyDecodeError(String),
    #[error("Key string error {0}")]
    KeyStringError(#[from] std::io::Error),

    // Value
    #[error("Value encode error {0}")]
    ValueEncodeError(String),
    #[error("Value decode error {0}")]
    ValueDecodeError(String),
    #[error("Value string error {0}")]
    ValueStringError(std::io::Error),

    // Indices
    #[error("Index Error: {0}")]
    IndexCreateError(String),
    #[error("Index Error: {0}")]
    IndexDeleteError(String),
}

#[derive(Error, Debug)]
pub(crate) enum ScanError {
    #[error("{0}")]
    SeekError(String),
    #[error("{0}")]
    PredicateError(String),
    #[error("{0}")]
    InvalidRangeError(String),
    #[error("{0}")]
    ScanCreateError(String),
    #[error("{0}")]
    IterCreateError(String),
}

#[derive(Error, Debug)]
pub(crate) enum TXError {
    #[error("write function called on read TX")]
    MismatchedKindError,
    #[error("key range error")]
    KeyRangeError,

    // transaction trait errors
    #[error("abort error: {0}")]
    AbortError(String),
    #[error("initialize error {0}")]
    TxBeginError(String),
    #[error("Commit error {0}")]
    CommitError(String),
    #[error("Retry limit reached")]
    RetriesExceeded,
}
