use std::{convert::Infallible, fmt::Display, io};

#[derive(Debug)]
pub enum Error {
    IndexError,
    FileError(io::Error),
    PointerError(String),
    CastingError,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IndexError => write!(f, "Index error"),
            Error::FileError(e) => write!(f, "File error: {}", e),
            Error::PointerError(e) => write!(f, "Pointer error: {}", e),
            Error::CastingError => write!(f, "Type casting error"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::FileError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::FileError(err)
    }
}

