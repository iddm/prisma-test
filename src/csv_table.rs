//! A table abstraction using CSV.

use csv::Reader;
use std::{collections::HashMap, error::Error};

use crate::{
    filter::{ApplyTableFilterByValue, FilterColumns, FilterQueryIterator},
    table::{AsTable, ColumnValue},
};

#[derive(Debug)]
pub struct CsvTable {
    data: HashMap<String, Vec<ColumnValue>>,
}

impl CsvTable {
    // Load CSV data into memory
    pub fn from_csv(file_path: &str) -> Result<Self, Box<dyn Error>> {
        let mut rdr = Reader::from_path(file_path)?;
        let headers: Vec<String> = rdr.headers()?.iter().map(|h| h.to_string()).collect();
        let mut data = HashMap::new();

        for result in rdr.records() {
            let record = result?;
            for (i, value) in record.iter().enumerate() {
                data.entry(headers[i].clone())
                    .or_insert_with(Vec::new)
                    .push(value.parse()?);
            }
        }

        Ok(CsvTable { data })
    }

    /// Queries the table with a filter and prints out the result to
    /// the stdout.
    pub fn query<F, E>(&self, filter_columns: F) -> crate::error::Result<(), E>
    where
        FilterColumns: TryFrom<F, Error = E>,
    {
        let filter_columns = FilterColumns::try_from(filter_columns)?;

        self.apply_filter(&filter_columns).for_each(|row| {
            for (col_name, value) in row {
                print!("{}: {} ", col_name, value);
            }
            println!()
        });

        Ok(())
    }
}

impl ApplyTableFilterByValue<'_> for CsvTable {
    fn apply_filter(&self, filter: &FilterColumns) -> FilterQueryIterator {
        FilterQueryIterator::new(Box::new(self.get_rows()), filter.clone())
    }
}

impl AsTable for CsvTable {
    fn get_name(&self) -> &str {
        "CSV Table (in-memory)"
    }

    fn get_columns(&self) -> impl Iterator<Item = (&str, &[ColumnValue])> {
        self.data
            .iter()
            .map(|(name, values)| (name.as_str(), values.as_slice()))
    }

    fn get_column_names(&self) -> impl Iterator<Item = &String> {
        self.data.keys()
    }

    // Iterator over rows without collecting into vectors
    fn get_rows(&self) -> Box<dyn Iterator<Item = HashMap<String, &ColumnValue>> + '_> {
        // Assume all columns have the same number of rows, get the number of rows from the first column
        let num_rows = if let Some(first_column) = self.data.values().next() {
            first_column.len()
        } else {
            0
        };

        // Return a row iterator using indexing
        Box::new((0..num_rows).map(move |row_idx| {
            let mut row = HashMap::new();
            for (col_name, col_values) in &self.data {
                if let Some(value) = col_values.get(row_idx) {
                    row.insert(col_name.clone(), value);
                }
            }
            row
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::table::{ColumnType, IntegerColumnType, StringColumnType};

    use super::*;

    fn create_csv_table() -> CsvTable {
        let data = vec![
            ("col1", ColumnValue::Integer(IntegerColumnType(1))),
            (
                "col2",
                ColumnValue::String(StringColumnType("value1".to_string())),
            ),
            ("col1", ColumnValue::Integer(IntegerColumnType(2))),
            (
                "col2",
                ColumnValue::String(StringColumnType("value2".to_string())),
            ),
        ];

        let mut table = CsvTable {
            data: HashMap::new(),
        };

        for (col_name, value) in data {
            table
                .data
                .entry(col_name.to_string())
                .or_default()
                .push(value);
        }

        table
    }

    #[test]
    fn create_table() {
        let table = create_csv_table();
        assert_eq!(table.get_name(), "CSV Table (in-memory)");

        let columns: Vec<(&str, &[ColumnValue])> = table.get_columns().collect();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].0, "col1");
        assert_eq!(columns[0].1.len(), 2);
        assert_eq!(columns[0].1[0].get_type(), ColumnType::Integer);
        assert_eq!(columns[0].1[1].get_type(), ColumnType::Integer);
        assert_eq!(columns[1].0, "col2");
        assert_eq!(columns[1].1.len(), 2);
        assert_eq!(columns[1].1[0].get_type(), ColumnType::String);
        assert_eq!(columns[1].1[1].get_type(), ColumnType::String);

        let column_names: Vec<&String> = table.get_column_names().collect();
        assert_eq!(column_names.len(), 2);
        assert_eq!(column_names[0], "col1");
        assert_eq!(column_names[1], "col2");
    }

    #[test]
    fn get_rows() {
        use crate::table::ColumnType;

        let table = create_csv_table();

        let rows: Vec<HashMap<String, &ColumnValue>> = table.get_rows().collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 2);
        assert_eq!(rows[0]["col1"].get_type(), ColumnType::Integer);
        assert_eq!(rows[0]["col1"].as_string(), None);
        assert_eq!(rows[0]["col2"].get_type(), ColumnType::String);
        assert_eq!(
            rows[0]["col2"].as_string(),
            Some(&StringColumnType("value1".to_string()))
        );

        assert_eq!(rows[1].len(), 2);
        assert_eq!(rows[1]["col1"].get_type(), ColumnType::Integer);
        assert_eq!(rows[1]["col1"].as_string(), None);
        assert_eq!(rows[1]["col2"].get_type(), ColumnType::String);
        assert_eq!(
            rows[1]["col2"].as_string(),
            Some(&StringColumnType("value2".to_string()))
        );
    }

    #[test]
    fn filter() {
        use crate::filter::{FilterByValue, Operation};

        let table = create_csv_table();

        let filter_columns = FilterColumns {
            output_columns: vec!["col1".to_string()],
            filters: vec![(
                "col2".to_string(),
                FilterByValue {
                    operation: Operation::Equal,
                    value: ColumnValue::String(StringColumnType("value1".to_string())),
                },
            )]
            .into_iter()
            .collect(),
        };

        let filtered_iter = table.apply_filter(&filter_columns);
        let filtered_rows: Vec<HashMap<String, &ColumnValue>> = filtered_iter.collect();

        assert_eq!(filtered_rows.len(), 1);
        assert_eq!(filtered_rows[0].len(), 1);
        assert_eq!(filtered_rows[0]["col1"].get_type(), ColumnType::Integer);
        assert_eq!(filtered_rows[0]["col1"].as_string(), None);
    }
}
