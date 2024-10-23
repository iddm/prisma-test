//! An abstract table representation and query engine.
#![allow(dead_code)]

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;

use crate::error::Result;
use crate::filter::{ApplyColumnFilterByValue, FilterByValue};

/// The integers in the data table.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct IntegerColumnType(pub i64);

impl std::fmt::Display for IntegerColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl ApplyColumnFilterByValue for IntegerColumnType {
    fn apply_filter_by_value(&self, filter: &FilterByValue) -> Result<bool> {
        let value = match filter.value {
            ColumnValue::Integer(value) => value,
            _ => {
                return Err(crate::error::FilterError::InvalidFilterValueType.into());
            }
        };

        Ok(match filter.operation {
            crate::filter::Operation::Equal => self == &value,
            crate::filter::Operation::GreaterThan => self > &value,
            crate::filter::Operation::LessThan => self < &value,
        })
    }
}

impl Deref for IntegerColumnType {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for IntegerColumnType {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromStr for IntegerColumnType {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(IntegerColumnType(s.parse()?))
    }
}

impl From<i64> for IntegerColumnType {
    fn from(i: i64) -> Self {
        IntegerColumnType(i)
    }
}

/// The string column type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct StringColumnType(pub String);

impl ApplyColumnFilterByValue for StringColumnType {
    fn apply_filter_by_value(&self, filter: &FilterByValue) -> Result<bool> {
        let value = match &filter.value {
            ColumnValue::String(value) => value,
            _ => {
                return Err(crate::error::FilterError::InvalidFilterValueType.into());
            }
        };

        Ok(match filter.operation {
            crate::filter::Operation::Equal => self == value,
            crate::filter::Operation::GreaterThan => self > value,
            crate::filter::Operation::LessThan => self < value,
        })
    }
}

impl std::fmt::Display for StringColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl Deref for StringColumnType {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StringColumnType {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromStr for StringColumnType {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(StringColumnType(s.to_string()))
    }
}

impl From<String> for StringColumnType {
    fn from(s: String) -> Self {
        StringColumnType(s)
    }
}

impl From<&str> for StringColumnType {
    fn from(s: &str) -> Self {
        StringColumnType(s.to_string())
    }
}

impl From<&String> for StringColumnType {
    fn from(s: &String) -> Self {
        StringColumnType(s.to_string())
    }
}

/// Represents the type of a column in the data table.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ColumnType {
    /// The cell is an integer.
    Integer,
    /// The cell is a string.
    String,
}

/// A single column value.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub enum ColumnValue {
    /// The cell contains an integer.
    Integer(IntegerColumnType),
    /// The cell contains a string.
    String(StringColumnType),
}

impl ColumnValue {
    /// Returns the value as an integer if it is an integer.
    pub fn as_integer(&self) -> Option<IntegerColumnType> {
        match self {
            ColumnValue::Integer(value) => Some(*value),
            _ => None,
        }
    }

    /// Returns the value as a string if it is a string.
    pub fn as_string(&self) -> Option<&StringColumnType> {
        match self {
            ColumnValue::String(value) => Some(value),
            _ => None,
        }
    }

    /// Returns the column type of the value.
    pub fn get_type(&self) -> ColumnType {
        match self {
            ColumnValue::Integer(_) => ColumnType::Integer,
            ColumnValue::String(_) => ColumnType::String,
        }
    }
}

impl FromStr for ColumnValue {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self> {
        if let Ok(value) = s.parse::<IntegerColumnType>() {
            Ok(ColumnValue::Integer(value))
        } else {
            Ok(ColumnValue::String(StringColumnType(s.to_string())))
        }
    }
}

impl std::fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnValue::Integer(value) => write!(f, "{value}"),
            ColumnValue::String(value) => write!(f, "{value}"),
        }
    }
}

impl ApplyColumnFilterByValue for ColumnValue {
    fn apply_filter_by_value(&self, filter: &FilterByValue) -> Result<bool> {
        match self {
            ColumnValue::Integer(value) => value.apply_filter_by_value(filter),
            ColumnValue::String(value) => value.apply_filter_by_value(filter),
        }
    }
}

/// A trait for representing a table.
pub trait AsTable {
    /// Returns the name of the table.
    fn get_name(&self) -> &str;

    /// Returns the columns of the table. The first element of the tuple
    /// is the column name, and the second element is the column values.
    fn get_columns(&self) -> impl Iterator<Item = (&str, &[ColumnValue])>;

    /// Returns the names of the columns in the table.
    fn get_column_names(&self) -> impl Iterator<Item = &String>;

    /// Returns an iterator over the values in the table.
    fn get_values(&self) -> impl Iterator<Item = (&str, &ColumnValue)> {
        self.get_columns()
            .flat_map(|(name, values)| values.iter().map(move |value| (name, value)))
    }

    /// Returns an iterator over the rows in the table.
    fn get_rows(&self) -> Box<dyn Iterator<Item = HashMap<String, &ColumnValue>> + '_>;
}
