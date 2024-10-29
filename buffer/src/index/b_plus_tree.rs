#![allow(dead_code, unused_variables)]

use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use storage::{SizeHelper, BPLUS_TREE_INTERNAL_PAGE_HEADER_SIZE, BPLUS_TREE_LEAF_PAGE_HEADER_SIZE};

use crate::BufferPoolManager;

/// Main class providing the API for the Interactive B+ Tree.
pub struct BPlusTree<KeyType, ValueType, KeyComparator> {
    index_name: String,
    bpm: Arc<BufferPoolManager>,
    comparator: KeyComparator,

    log: Vec<String>,
    leaf_max_size: u32,
    internal_max_size: u32,

    header_page_id: usize,
    k: PhantomData<KeyType>,
    v: PhantomData<ValueType>,
}

impl<KeyType, ValueType, KeyComparator> BPlusTree<KeyType, ValueType, KeyComparator> {
    pub fn new(
        index_name: String,
        header_page_id: usize,
        bpm: Arc<BufferPoolManager>,
        comparator: KeyComparator,
        leaf_max_size: Option<u32>,
        internal_max_size: Option<u32>,
    ) -> Self {
        let leaf_page_size = SizeHelper::get_internal_page_slot_cnt::<
            BPLUS_TREE_LEAF_PAGE_HEADER_SIZE,
            KeyType,
            ValueType,
        >() as u32;
        let internal_page_size = SizeHelper::get_internal_page_slot_cnt::<
            BPLUS_TREE_INTERNAL_PAGE_HEADER_SIZE,
            KeyType,
            ValueType,
        >() as u32;
        Self {
            index_name,
            bpm,
            comparator,
            log: vec![],
            leaf_max_size: leaf_max_size.unwrap_or(leaf_page_size),
            internal_max_size: internal_max_size.unwrap_or(internal_page_size),
            header_page_id,
            k: PhantomData,
            v: PhantomData,
        }
    }
    pub fn is_empty() -> bool {
        todo!()
    }

    pub fn insert(&mut self, key: KeyType, value: ValueType) -> bool {
        true
    }

    /// Return the only value that associated with input key.
    /// This method is used for point query
    /// @return : true means key exists
    pub fn get_value(&self, key: KeyType, result: &mut Vec<ValueType>) -> bool {
        true
    }

    pub fn remove(&mut self, key: impl Borrow<KeyType>) {}
}

#[cfg(test)]
mod test {
    use std::{marker::PhantomData, sync::Arc};

    use catalog::parse_create_stmt;
    use common::RID;
    use storage::MemoryManager;

    use crate::{index::GenericKey, BufferPoolManager};

    use super::BPlusTree;

    #[test]
    fn insert_test_1() {
        let key_schema = parse_create_stmt("a bigint");
        let disk_manager = MemoryManager::new(1000);
        let bpm = Arc::new(BufferPoolManager::new(50, 10, Box::new(disk_manager)));

        let page_id = bpm.new_page_id();
        let mut tree = BPlusTree::<GenericKey<8>, RID, PhantomData<u32>>::new(
            "foo_pk".into(),
            page_id,
            bpm.clone(),
            PhantomData,
            Some(2),
            Some(3),
        );

        let key = 42;
        let value = key & 0xFFFFFFFF;
        let rid = RID::new(key, value as u32);
        let index_key: GenericKey<8> = key.into();
        tree.insert(index_key, rid);

        let root_page_id = tree.header_page_id;
        let root_page_guard = bpm.read_page(root_page_id).unwrap();
    }

    #[test]
    fn insert_test_2() {
        let key_schema = parse_create_stmt("a bigint");
        let disk_manager = MemoryManager::new(1000);
        let bpm = Arc::new(BufferPoolManager::new(50, 10, Box::new(disk_manager)));

        let page_id = bpm.new_page_id();
        let mut tree = BPlusTree::<GenericKey<8>, RID, PhantomData<u32>>::new(
            "foo_pk".into(),
            page_id,
            bpm.clone(),
            PhantomData,
            Some(2),
            Some(3),
        );

        for key in [1, 2, 3, 4, 5] {
            let slot_num = key & 0xFFFFFFFF;
            tree.insert(key.into(), RID::new(key >> 32, slot_num as u32));
        }

        for key in [1, 2, 3, 4, 5] {
            let mut rids = Vec::new();
            let index_key: GenericKey<8> = key.into();
            let is_present = tree.get_value(index_key, &mut rids);

            // assert_eq!(true, is_present);
            // assert_eq!(1, rids.len());
            // assert_eq!(0, rids[0].page_id);
            // let slot_num = key & 0xFFFFFFFF;
            // assert_eq!(slot_num as u32, rids[0].slot_num);
        }
    }
}
