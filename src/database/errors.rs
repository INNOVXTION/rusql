use std::{
    fmt::{Debug, Display},
    io,
    num::TryFromIntError,
    str::Utf8Error,
};

use rustix::io::Errno;

#[derive(Debug)]
pub enum Error {
    IndexError,
    FileError(io::Error),
    IntCastError(Option<TryFromIntError>),
    StrCastError(Utf8Error),
    SplitError(String),
    MergeError(String),
    InsertError(String),
    DeleteError(String),
    PagerError(PagerError),
    PagerSetError,
    InvalidInput(&'static str),
    FreeListError(FLError),
    SearchError(String),
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
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::FileError(err)
    }
}
impl From<TryFromIntError> for Error {
    fn from(err: TryFromIntError) -> Self {
        Error::IntCastError(Some(err))
    }
}
impl From<Utf8Error> for Error {
    fn from(value: Utf8Error) -> Self {
        Error::StrCastError(value)
    }
}

#[derive(Debug)]
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

impl From<FLError> for Error {
    fn from(value: FLError) -> Self {
        Error::FreeListError(value)
    }
}

impl std::error::Error for PagerError {}

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

#[derive(Debug)]
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

impl std::error::Error for FLError {}

#[derive(Debug)]
pub(crate) enum TableError {
    RecordError(String),
    TableError(String),
    CellError(String),
    CodecError(String),
}

impl Display for TableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableError::RecordError(s) => write!(f, "Record Error {}", s),
            TableError::TableError(s) => write!(f, "Table Error {}", s),
            TableError::CellError(s) => write!(f, "Cell Error {}", s),
            TableError::CodecError(s) => write!(f, "Codec Error {}", s),
        }
    }
}

impl std::error::Error for TableError {}
