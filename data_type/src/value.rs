#![allow(dead_code)]

/// A value represents a view over SQL data stored in
/// some materialized state. All values have a type and comparison functions, but
/// subclasses implement other type-specific functionality.
#[derive(Debug, Clone)]
pub enum Value {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Decimal(f64),
    Timestamp(u64),
    Varchar(String),
    Vector(Vec<f64>),
}

macro_rules! make_get_value {
    ($name:ident, $input: ty, $output: expr) => {
        impl Value {
            pub fn $name(value: $input) -> Value {
                $output(value)
            }
        }
    };
}

make_get_value!(get_boolean_value, bool, Value::Boolean);
make_get_value!(get_tiny_int_value, i8, Value::TinyInt);
make_get_value!(get_small_int_value, i16, Value::SmallInt);
make_get_value!(get_integer_value, i32, Value::Integer);
make_get_value!(get_big_int_value, i64, Value::BigInt);
make_get_value!(get_decimal_value, f64, Value::Decimal);
make_get_value!(get_timestamp_value, u64, Value::Timestamp);
make_get_value!(get_varchar_value, String, Value::Varchar);
make_get_value!(get_vector_value, Vec<f64>, Value::Vector);

#[cfg(test)]
mod test {
    use super::Value;

    #[test]
    fn get_integer_value() {
        let num = Value::get_integer_value(0);
        println!("{:?}", num);
    }

    #[test]
    fn get_vector_value() {
        let nums = Value::get_vector_value(vec![1., 2.]);
        println!("{:?}", nums);
    }

    #[test]
    fn get_varchar() {
        let name = Value::get_varchar_value(String::from("hello"));
        println!("{name:?}")
    }
}
