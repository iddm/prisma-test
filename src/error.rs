//! Error types.

use std::num::ParseIntError;

/// A specialized [`Result`] type for this crate.
pub type Result<T = (), E = Error> = std::result::Result<T, E>;

/// A filter error which can occur when applying a filter.
#[derive(Debug, Clone)]
pub enum FilterError {
    /// The filter operation is invalid due to having an invalid value
    /// type.
    InvalidFilterValueType,
    /// If for some reason the values cannot be compared. For example,
    /// two floating point values are NaN.
    ValuesCannotBeCompared,
    /// A filter parse error.
    ParseError(String),
}

impl std::fmt::Display for FilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFilterValueType => {
                write!(
                    f,
                    "The filter operation is invalid due to having an invalid value type"
                )
            }
            Self::ValuesCannotBeCompared => {
                write!(f, "The values cannot be compared")
            }
            Self::ParseError(e) => {
                write!(f, "Parse error: {e}")
            }
        }
    }
}

impl std::error::Error for FilterError {}

/// An error which can occur within the crate.
#[derive(Debug)]
pub enum Error {
    /// A value parse error.
    ValueParseError(String),
    /// A filter error.
    FilterError(FilterError),
    /// Any other error type.
    Other(Box<dyn std::error::Error>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ValueParseError(e) => write!(f, "Value parse error: {e}"),
            Self::FilterError(e) => write!(f, "Filter error: {e}"),
            Self::Other(e) => write!(f, "Other error: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<FilterError> for Error {
    fn from(e: FilterError) -> Self {
        Self::FilterError(e)
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        Self::Other(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Other(Box::new(e))
    }
}

impl From<csv::Error> for Error {
    fn from(e: csv::Error) -> Self {
        Self::Other(Box::new(e))
    }
}

impl From<ParseIntError> for Error {
    fn from(e: ParseIntError) -> Self {
        Self::ValueParseError(e.to_string())
    }
}
