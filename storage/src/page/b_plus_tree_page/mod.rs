#![allow(dead_code)]

use b_plus_tree_internal_page::BPlusTreeInternalPage;
use b_plus_tree_leaf_page::BPlusTreeLeafPage;
use serde::Serialize;

#[repr(C)]
enum IndexPageType {
    InvalidIndexPage,
    LeafPage,
    InternalPage,
}

impl Serialize for IndexPageType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let val: u32 = match self {
            IndexPageType::InvalidIndexPage => 1,
            IndexPageType::LeafPage => 2,
            IndexPageType::InternalPage => 3,
        };
        serializer.serialize_u32(val)
    }
}

pub struct SizeHelper;

impl SizeHelper {
    pub fn get_internal_page_slot_cnt<const N: usize, KeyType, ValueType>() -> usize {
        (4096 - N - 8)
            / (std::mem::size_of::<KeyType>()
                + std::mem::size_of::<ValueType>()
                // vec takes space for len and capacity. And we have two vec, one for key and one for value. 
                + 2 * 4 * std::mem::size_of::<usize>())
    }
}

macro_rules! keys_str {
    (
        $name: ident
    ) => {
        impl<KeyType: std::fmt::Debug, ValueType: std::fmt::Debug> ToString
            for $name<KeyType, ValueType>
        {
            fn to_string(&self) -> String {
                let mut k_str = String::from("(");

                self.keys.iter().enumerate().skip(1).for_each(|it| {
                    if it.0 != 0 {
                        k_str.push_str(",");
                    }

                    k_str.push_str(&format!("{:?}", it.1));
                });

                k_str
            }
        }
    };
}

pub struct BplusTreeHeaderPage(usize);

pub mod b_plus_tree_internal_page;
pub mod b_plus_tree_leaf_page;

///
/// Both internal and leaf page are inherited from this page.
///
/// It actually serves as a header part for each B+ tree page and
/// contains information shared by both leaf page and internal page.
///
/// Header format (size in byte, 12 bytes in total):
/// ---------------------------------------------------------
/// | PageType (4) | CurrentSize (4) | MaxSize (4) |  ...   |
/// ---------------------------------------------------------
///
#[repr(C)]
pub enum BPlusTreePage<KeyType, ValueType> {
    BPlusTreeLeafPage {
        header: BPlusTreeLeafHeader,
        data: BPlusTreeLeafPage<KeyType, ValueType>,
    },
    BPlusTreeInternalPage {
        header: BPlusTreeInternalHeader,
        data: BPlusTreeInternalPage<KeyType, ValueType>,
    },
}

#[repr(C)]
pub struct BPlusTreeInternalHeader {
    size: u32,
    max_size: u32,
}

#[repr(C)]
pub struct BPlusTreeLeafHeader {
    size: u32,
    max_size: u32,
    next_page_id: usize,
}

#[cfg(test)]
mod test {
    use catalog::parse_create_stmt;

    #[test]
    fn insert_test_1() {
        let key_schema = parse_create_stmt("a bigint");
        println!("{key_schema:?}");
    }
}
