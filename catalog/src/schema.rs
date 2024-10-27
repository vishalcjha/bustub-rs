#![allow(dead_code)]

use crate::Column;

#[derive(Debug)]
pub struct Schema {
    columns: Vec<Column>,
    /** Indices of all non inlined columns. */
    non_inlined_columns: Vec<usize>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns,
            non_inlined_columns: vec![],
        }
    }
    pub fn get_col_idx(&self, name: impl AsRef<str>) -> Option<u32> {
        self.columns
            .iter()
            .enumerate()
            .find(|it| it.1.get_name().eq(name.as_ref()))
            .map(|it| it.0 as u32)
    }

    pub fn get_col_count(&self) -> u32 {
        self.columns.len() as u32
    }

    pub fn get_non_inlined_column_count(&self) -> u32 {
        self.non_inlined_columns.len() as u32
    }
}
