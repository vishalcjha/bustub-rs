#![allow(dead_code)]

use std::{array, usize};

mod b_plus_tree;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct GenericKey<const N: usize> {
    data: [char; N],
}

/// Only for testing
impl<const N: usize> From<usize> for GenericKey<N> {
    fn from(value: usize) -> Self {
        let value = value.to_string();
        let data: [char; N] = array::from_fn(|i| value.chars().nth(i).unwrap_or('0'));

        Self { data }
    }
}

#[cfg(test)]
mod test {
    use super::GenericKey;

    #[test]
    fn generic_key_from_usize() {
        let key: GenericKey<4> = 20usize.into();
        assert_eq!(key.data, ['2', '0', '0', '0']);
    }
}
