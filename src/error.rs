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
    #[allow(dead_code)]
    ValuesCannotBeCompared,
    /// A filter parse error.
    Parse(String),
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
            Self::Parse(e) => {
                write!(f, "Parsing failed: {e}")
            }
        }
    }
}

impl std::error::Error for FilterError {}

/// An error which can occur within the crate.
#[derive(Debug)]
pub enum Error {
    /// A value parse error.
    ValueParse(String),
    /// A filter error.
    Filter(FilterError),
    /// Any other error type.
    Other(Box<dyn std::error::Error>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ValueParse(e) => write!(f, "Value parse: {e}"),
            Self::Filter(e) => write!(f, "Filter: {e}"),
            Self::Other(e) => write!(f, "Other: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<FilterError> for Error {
    fn from(e: FilterError) -> Self {
        Self::Filter(e)
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
        Self::ValueParse(e.to_string())
    }
}
