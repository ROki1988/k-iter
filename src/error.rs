use failure::{Backtrace, Context, Fail};
use rusoto_core::RusotoError;
use rusoto_kinesis::{GetRecordsError, GetShardIteratorError, ListShardsError};
use std::fmt;
use std::fmt::Display;

use tokio::time::Error as TimeError;

#[derive(Fail, Debug)]
pub enum ErrorKind {
    #[fail(display = "Rusoto error")]
    Rusoto,
    #[fail(display = "Tokio error")]
    Tokio,
}

#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>,
}

impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

#[allow(dead_code)]
impl Error {
    pub fn new(inner: Context<ErrorKind>) -> Error {
        Error { inner }
    }

    pub fn kind(&self) -> &ErrorKind {
        self.inner.get_context()
    }
}

#[allow(dead_code)]
impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error {
            inner: Context::new(kind),
        }
    }
}

impl From<RusotoError<GetShardIteratorError>> for Error {
    fn from(error: RusotoError<GetShardIteratorError>) -> Error {
        Error {
            inner: error.context(ErrorKind::Rusoto),
        }
    }
}

impl From<RusotoError<GetRecordsError>> for Error {
    fn from(error: RusotoError<GetRecordsError>) -> Error {
        Error {
            inner: error.context(ErrorKind::Rusoto),
        }
    }
}

impl From<RusotoError<ListShardsError>> for Error {
    fn from(error: RusotoError<ListShardsError>) -> Error {
        Error {
            inner: error.context(ErrorKind::Rusoto),
        }
    }
}

impl From<TimeError> for Error {
    fn from(error: TimeError) -> Error {
        Error {
            inner: error.context(ErrorKind::Tokio),
        }
    }
}
