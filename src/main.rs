use std::{error::Error, io::Write};

use csv_table::CsvTable;
use filter::FilterColumns;

mod csv_table;
mod error;
mod filter;
mod table;

fn manually() -> Result<(), Box<dyn Error>> {
    let data_table = csv_table::CsvTable::from_csv("data.csv")?;

    let projection = vec!["col1".to_owned(), "col2".to_owned()];

    data_table
        .query(FilterColumns {
            output_columns: projection,
            filters: vec![(
                "col3".to_string(),
                filter::FilterByValue {
                    operation: filter::Operation::GreaterThan,
                    value: "5".parse().unwrap(),
                },
            )]
            .into_iter()
            .collect(),
        })
        .expect("Query failed");

    Ok(())
}

fn with_parser() -> Result<(), Box<dyn Error>> {
    let data_table = csv_table::CsvTable::from_csv("data.csv")?;

    let query = "PROJECT col1, col2 FILTER col3 > 5";

    let filter = filter::parse_filter_query(query)?;

    data_table.query(filter).expect("Query failed");

    Ok(())
}

fn repl_loop(data_table: CsvTable) -> Result<(), Box<dyn Error>> {
    println!("Welcome to the CSV data query tool!");

    loop {
        print!("\n (CTRL-C for exit) REPL > ");
        let _ = std::io::stdout().flush();

        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;

        println!();

        let filter = match filter::parse_filter_query(&input) {
            Ok(filter) => filter,
            Err(e) => {
                eprintln!("Parsing error occured: {e}");
                continue;
            }
        };

        if let Err(e) = data_table.query(filter) {
            eprintln!("Error occured: {e}");
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let data_table = csv_table::CsvTable::from_csv("data.csv")?;

    repl_loop(data_table)?;

    Ok(())
}
