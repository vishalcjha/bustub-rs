#[allow(dead_code)]
// pub trait DataType {
//     fn get_type_size(&self) -> u64;
//     fn is_coercible_from(&self, _other: &dyn DataType) -> bool {
//         return false;
//     }
// }
mod value;
use std::{fmt::Display, str::FromStr};

/// Every possible SQL type ID
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataType {
    Invalid,
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Decimal,
    Varchar(u32),
    Timestamp,
    Vector,
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl FromStr for DataType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = match s {
            "bool" | "boolean" => DataType::Boolean,
            "tinyint" => DataType::TinyInt,
            "smallint" => DataType::SmallInt,
            "int" | "integer" => DataType::Integer,
            "bigint" => DataType::BigInt,
            "double" | "float" => DataType::Decimal,
            x if x.starts_with("varchar") || x.starts_with("char") => {
                let Some(start_of_size) = x.find('(') else {
                    return Err(());
                };
                let Some(end_of_size) = x.find(')') else {
                    return Err(());
                };

                let Ok(size) = &x[start_of_size + 1..end_of_size].parse::<u32>() else {
                    return Err(());
                };
                DataType::Varchar(*size)
            }
            _ => DataType::Integer,
        };
        Ok(res)
    }
}

impl DataType {
    pub fn is_numeric(&self) -> bool {
        use DataType::*;
        matches!(self, TinyInt | SmallInt | Integer | BigInt)
    }
}

#[cfg(test)]
mod test {
    use crate::DataType;

    #[test]
    fn display_for_data_type() {
        let data_type = DataType::Invalid;
        println!("{}", data_type);
    }
}
