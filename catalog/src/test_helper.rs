#![allow(dead_code)]

use std::str::FromStr;

use data_type::DataType;

use crate::{schema::Schema, Column};

pub fn parse_create_stmt(stmt: impl Into<String>) -> Schema {
    let stmt: String = stmt.into();
    let stmt = stmt.to_lowercase();

    let mut columns = Vec::new();
    for token in stmt.split(',') {
        let n = token.find(' ').unwrap();
        let column_name = &token[..n];
        let column_type = &token[n + 1..];

        let column_type = DataType::from_str(column_type).unwrap();
        columns.push(Column::new(column_name, column_type));
    }

    Schema::new(columns)
}
