use std::{
    fmt::{Debug, Display},
    io,
    num::TryFromIntError,
    str::Utf8Error,
};

use crate::database::tables::TypeCol;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    IndexError,
    SplitError(String),
    MergeError(String),
    InsertError(String),
    DeleteError(String),
    PagerSetError,
    InvalidInput(&'static str),
    SearchError(String),

    PagerError(#[from] PagerError),
    FreeListError(#[from] FLError),
    TableError(#[from] TableError),
    ScanError(#[from] ScanError),
    TransactionError(#[from] TXError),

    StrCastError(#[from] Utf8Error),
    IntCastError(#[from] Option<TryFromIntError>),

    FileError(#[from] io::Error),
    SysFileError(#[from] rustix::io::Errno),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Error as E;
        match self {
            E::IndexError => write!(f, "Index error"),
            E::FileError(e) => write!(f, "File error: {e}"),
            E::IntCastError(Some(e)) => write!(f, "Type casting error, {e}"),
            E::IntCastError(None) => write!(f, "Type casting error"),
            E::StrCastError(e) => write!(f, "Casting from String error, {e}"),
            E::SplitError(e) => write!(f, "Error when splitting, {e}"),
            E::InsertError(e) => write!(f, "Error when inserting, {e}"),
            E::MergeError(e) => write!(f, "Error when merging, {e}"),
            E::DeleteError(e) => write!(f, "Error when deleting {e}"),
            E::PagerError(e) => write!(f, "Error when calling pager {e}"),
            E::PagerSetError => write!(f, "Attempting to set global pager again!"),
            E::InvalidInput(e) => write!(f, "invalid input!, {e}"),
            E::FreeListError(e) => write!(f, "Free List Error {e}"),
            E::SearchError(e) => write!(f, "Search Error {e}"),
            E::TableError(e) => write!(f, "Table Error {e}"),
            E::SysFileError(e) => write!(f, "Errno {e}"),
            E::ScanError(e) => write!(f, "Scan error {e}"),
            E::TransactionError(e) => write!(f, "transaction error {e}"),
        }
    }
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

// #[error("{var}")]    ⟶   write!("{}", self.var)
// #[error("{0}")]      ⟶   write!("{}", self.0)
// #[error("{var:?}")]  ⟶   write!("{:?}", self.var)
// #[error("{0:?}")]    ⟶   write!("{:?}", self.0)

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
    #[error("commit error: {0}")]
    CommitError(String),
    #[error("abort error: {0}")]
    AbortError(String),
    #[error("initialize error {0}")]
    TxBeginError(String),
}
