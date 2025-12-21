use std::{
    fmt::{Debug, Display},
    io,
    num::TryFromIntError,
    str::Utf8Error,
};

use crate::database::tables::{DataCell, TypeCol};

use rustix::io::Errno;
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

    StrCastError(#[from] Utf8Error),
    IntCastError(#[from] Option<TryFromIntError>),

    FileError(#[from] io::Error),
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
        }
    }
}

#[derive(Debug, Error)]
pub enum PagerError {
    UnkownError,
    PageNotFound(u64),
    NoAvailablePage,
    DeallocError(u64),
    CodecError(io::Error),
    FDError(Errno),
    FileNameError,
    UnsupportedOS,
    UnalignedOffset(u64),
    UnalignedLength(usize),
    MMapError(Errno),
    WriteFileError(Errno),
}

impl Display for PagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PagerError::UnkownError => write!(f, "an unrecovable error occured"),
            PagerError::PageNotFound(e) => write!(f, "Couldnt retrieve page: {}", e),
            PagerError::NoAvailablePage => write!(f, "No free pages available"),
            PagerError::DeallocError(e) => write!(f, "Deallocation failed for page: {}", e),
            PagerError::CodecError(e) => write!(f, "Error when encoding/decoding node: {}", e),
            PagerError::FDError(e) => write!(f, "Error when handling file: {}", e),
            PagerError::FileNameError => {
                write!(f, "Invalid Filename, make sure it doesnt end with / ")
            }
            PagerError::UnsupportedOS => write!(f, "Page size but OS is not allowed!"),
            PagerError::UnalignedOffset(e) => write!(f, "Offset {} is invalid!", e),
            PagerError::UnalignedLength(e) => write!(f, "Length {} is invalid!", e),
            PagerError::MMapError(e) => write!(f, "Error when calling mmap {}", e),
            PagerError::WriteFileError(e) => write!(f, "Error when calling pwrite {}", e),
        }
    }
}

impl From<io::Error> for PagerError {
    fn from(value: io::Error) -> Self {
        Self::CodecError(value)
    }
}

impl From<rustix::io::Errno> for PagerError {
    fn from(value: rustix::io::Errno) -> Self {
        Self::FDError(value)
    }
}

#[derive(Debug, Error)]
pub enum FLError {
    UnknownError,
}

impl Display for FLError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FLError::UnknownError => write!(f, "an unkown error occured"),
        }
    }
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
    EncodeTableError(serde_json::Error),
    #[error("Delete table error {0}")]
    DecodeTableError(serde_json::Error),
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
}
