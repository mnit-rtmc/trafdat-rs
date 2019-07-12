// error.rs
//
// Copyright (c) 2019  Minnesota Department of Transportation
//
use std::fmt;
use std::io;
use std::net::AddrParseError;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    AddrParse(AddrParseError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{}", e),
            Error::AddrParse(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::AddrParse(e) => Some(e),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<AddrParseError> for Error {
    fn from(e: AddrParseError) -> Self {
        Error::AddrParse(e)
    }
}
