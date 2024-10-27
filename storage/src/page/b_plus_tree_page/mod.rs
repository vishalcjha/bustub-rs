#![allow(dead_code)]
mod b_plus_tree;
enum IndexPageType {
    InvalidIndexPage,
    LeafPage,
    InternalPage,
}
pub trait BPlusTree {
    fn get_size(&self) -> u64;
    fn set_size(&mut self, size: u64);

    fn get_max_size(&self) -> u64;
    fn set_max_size(&mut self, max_size: u64);

    fn get_min_size(&self) -> u64;
}

#[cfg(test)]
mod test {
    #[test]
    fn insert_test_1() {}
}
