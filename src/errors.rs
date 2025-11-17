use std::{fmt::Display, io, num::TryFromIntError, str::Utf8Error};

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
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IndexError => write!(f, "Index error"),
            Error::FileError(e) => write!(f, "File error: {e}"),
            Error::IntCastError(Some(e)) => write!(f, "Type casting error, {e}"),
            Error::IntCastError(None) => write!(f, "Type casting error"),
            Error::StrCastError(e) => write!(f, "Casting from String error, {e}"),
            Error::SplitError(e) => write!(f, "Error when splitting, {e}"),
            Error::InsertError(e) => write!(f, "Error when inserting, {e}"),
            Error::MergeError(e) => write!(f, "Error when merging, {e}"),
            Error::DeleteError(e) => write!(f, "Error when deleting {e}"),
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
