#![allow(dead_code)]
use data_type::DataType;

#[derive(Debug)]
pub struct Column {
    name: String,
    pub data_type: DataType,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
    pub fn get_name(&self) -> &str {
        &self.name
    }
}
