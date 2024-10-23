//! The filter operations.

use std::{collections::HashMap, str::FromStr};

use pest::Parser;
use pest_derive::Parser;

use crate::{
    error::{FilterError, Result},
    table::{ColumnValue, IntegerColumnType},
};

// The filter operations which can be performed in the engine.
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum Operation {
    /// To filter the two values which are equal to each other.
    Equal,
    // To filter the values which are greater than the other one.
    GreaterThan,
    // To filter the values which are less than the other one.
    LessThan,
}

impl Operation {
    /// Returns the string representation of the filter operation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Operation::Equal => "=",
            Operation::GreaterThan => ">",
            Operation::LessThan => "<",
        }
    }
}

impl FromStr for Operation {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "=" => Self::Equal,
            ">" => Self::GreaterThan,
            "<" => Self::LessThan,
            _ => return Err(FilterError::Parse(format!("Invalid filter operation: {s}")).into()),
        })
    }
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Represents the filter for a single value.
#[derive(Debug, Clone)]
pub struct FilterByValue {
    /// The filter operation to perform.
    pub operation: Operation,
    /// The value to compare against.
    pub value: ColumnValue,
}

/// Represents the filter for one or more columns.
#[derive(Debug, Clone)]
pub struct FilterColumns {
    /// The columns to return (the projection).
    pub output_columns: Vec<String>,
    /// The values to compare against. A map of column names to filters.
    pub filters: HashMap<String, FilterByValue>,
}

impl TryFrom<&str> for FilterColumns {
    type Error = crate::error::Error;

    fn try_from(value: &str) -> Result<Self> {
        parse_filter_query(value)
    }
}

impl TryFrom<String> for FilterColumns {
    type Error = crate::error::Error;

    fn try_from(value: String) -> Result<Self> {
        parse_filter_query(&value)
    }
}

impl TryFrom<&String> for FilterColumns {
    type Error = crate::error::Error;

    fn try_from(value: &String) -> Result<Self> {
        parse_filter_query(value)
    }
}

/// A trait for applying a filter to a column value.
pub trait ApplyColumnFilterByValue {
    /// Applies the filter to the column value. Returns [`true`] if the
    /// filter matches the value.
    fn apply_filter_by_value(&self, filter: &FilterByValue) -> Result<bool>;
}

pub trait ApplyTableFilterByValue<'a> {
    /// Applies the filter to the table. Returns an iterator over the
    /// filtered values.
    ///
    /// The first element of the tuple is the name of the column, and
    /// the second element is the column values.
    fn apply_filter(&'a self, filter: &FilterColumns) -> FilterQueryIterator<'a>;
}

/// A filter query iterator.
pub struct FilterQueryIterator<'a> {
    data: Box<dyn Iterator<Item = HashMap<String, &'a ColumnValue>> + 'a>,
    filter: FilterColumns,
}

impl<'a> FilterQueryIterator<'a> {
    /// Creates a new filter query iterator.
    pub fn new(
        data: Box<dyn Iterator<Item = HashMap<String, &'a ColumnValue>> + 'a>,
        filter: FilterColumns,
    ) -> Self {
        Self { data, filter }
    }
}

impl<'a> Iterator for FilterQueryIterator<'a> {
    type Item = HashMap<String, &'a ColumnValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.filter.output_columns.is_empty() {
            return None;
        }

        for row in self.data.by_ref() {
            let mut filtered_row = HashMap::new();
            let mut should_return = false;

            for (name, value) in row {
                let filter = self.filter.filters.get(&name);

                if let Some(filter) = filter {
                    should_return = value.apply_filter_by_value(filter).unwrap_or(false);
                }

                if self.filter.output_columns.contains(&name.to_string()) {
                    filtered_row.insert(name.clone(), value);
                }
            }

            if should_return {
                return Some(filtered_row);
            }
        }

        None
    }
}

#[derive(Parser)]
#[grammar_inline = r#"
// Main rules
query   = { project ~ filters }
project = { "PROJECT" ~ columns }
filters  = { "FILTER" ~ filter }
filter = { filter_expression ~ ("," ~ filter_expression)* }
filter_expression = { column ~ op ~ value }

// Main tokens
columns = { column ~ ("," ~ column)* }
column  = @{ ASCII_ALPHANUMERIC+ }
op      = @{ "<" | "=" | ">" }
value   = { ASCII_DIGIT+ | "\"" ~ ASCII_ALPHANUMERIC* ~ "\"" }

// Basic rules
WHITESPACE = _{ " "+ }
"#]
struct QueryParser;

/// Parses a filter query string into a [`FilterColumns`] struct.
pub fn parse_filter_query(input: &str) -> Result<FilterColumns> {
    let mut pairs =
        QueryParser::parse(Rule::query, input).map_err(|e| FilterError::Parse(e.to_string()))?;

    let mut output_columns = Vec::new();
    let mut filters = HashMap::new();

    // There should be a single pair representing the entire query
    let query_pair = pairs
        .next()
        .ok_or_else(|| FilterError::Parse("Expected query".to_string()))?;

    // Iterate over the inner pairs of the `query` rule
    for pair in query_pair.into_inner() {
        match pair.as_rule() {
            Rule::project => {
                for columns in pair.into_inner() {
                    if columns.as_rule() != Rule::columns {
                        return Err(FilterError::Parse("Expected columns".to_string()).into());
                    }

                    for column in columns.into_inner() {
                        if column.as_rule() == Rule::column {
                            output_columns.push(column.as_str().to_string());
                        }
                    }
                }
            }
            Rule::filters => {
                for filter in pair.into_inner() {
                    if filter.as_rule() != Rule::filter {
                        return Err(FilterError::Parse("Expected filter".to_string()).into());
                    }

                    for filter_expression in filter.into_inner() {
                        if filter_expression.as_rule() != Rule::filter_expression {
                            return Err(FilterError::Parse(
                                "Expected filter expression".to_string(),
                            )
                            .into());
                        }

                        let mut inner_rules = filter_expression.into_inner();

                        let col_name = inner_rules.next().unwrap().as_str().to_string();
                        let op = inner_rules.next().unwrap().as_str();
                        let value = inner_rules.next().unwrap().as_str();

                        let operation = Operation::from_str(op)?;
                        let column_value = if let Ok(int_value) = value.parse::<i64>() {
                            ColumnValue::Integer(IntegerColumnType(int_value))
                        } else {
                            ColumnValue::String(value.trim_matches('"').to_string().into())
                        };

                        filters.insert(
                            col_name,
                            FilterByValue {
                                operation,
                                value: column_value,
                            },
                        );
                    }
                }
            }
            _ => {}
        }
    }

    // Return the parsed FilterColumns
    Ok(FilterColumns {
        output_columns,
        filters,
    })
}

#[cfg(test)]
mod tests {
    use crate::table::StringColumnType;

    use super::*;

    #[test]
    fn parse_filter_query_succeeds() {
        let query = r#"PROJECT col1, col2 FILTER col1 = 5, col2 = "value""#;
        let filter = parse_filter_query(query).unwrap();

        assert_eq!(
            filter.output_columns,
            vec!["col1".to_string(), "col2".to_string()]
        );

        let col1_filter = filter.filters.get("col1").unwrap();
        assert_eq!(col1_filter.operation, Operation::Equal);
        assert_eq!(
            col1_filter.value,
            ColumnValue::Integer(IntegerColumnType(5))
        );

        let col2_filter = filter.filters.get("col2").unwrap();
        assert_eq!(col2_filter.operation, Operation::Equal);
        assert_eq!(
            col2_filter.value,
            ColumnValue::String(StringColumnType("value".to_string()))
        );
    }
}
