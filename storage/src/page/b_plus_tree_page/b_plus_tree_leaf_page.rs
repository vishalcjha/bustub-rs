use super::{BPlusTreeLeafHeader, SizeHelper};

///
/// Store indexed key and record id (record id = page id combined with slot id,
/// see `include/common/rid.h` for detailed implementation) together within leaf
/// page. Only support unique key.
///
/// Leaf page format (keys are stored in order):
///  ---------
/// | HEADER |
///  ---------
///  ---------------------------------
/// | KEY(1) | KEY(2) | ... | KEY(n) |
///  ---------------------------------
///  ---------------------------------
/// | RID(1) | RID(2) | ... | RID(n) |
///  ---------------------------------
///
///  Header format (size in byte, 16 bytes in total):
///  -----------------------------------------------
/// | PageType (4) | CurrentSize (4) | MaxSize (4) |
///  -----------------------------------------------
///  -----------------
/// | NextPageId (4) |
///  -----------------
///
#[repr(C)]
pub struct BPlusTreeLeafPage<KeyType, ValueType> {
    keys: Vec<KeyType>,
    values: Vec<ValueType>,
}

pub const BPLUS_TREE_LEAF_PAGE_HEADER_SIZE: usize = std::mem::size_of::<BPlusTreeLeafHeader>();
impl<KeyType, ValueType> BPlusTreeLeafPage<KeyType, ValueType> {
    fn new() -> Self {
        Self {
            keys: Vec::with_capacity(SizeHelper::get_internal_page_slot_cnt::<
                BPLUS_TREE_LEAF_PAGE_HEADER_SIZE,
                KeyType,
                ValueType,
            >()),
            values: Vec::with_capacity(SizeHelper::get_internal_page_slot_cnt::<
                BPLUS_TREE_LEAF_PAGE_HEADER_SIZE,
                KeyType,
                ValueType,
            >()),
        }
    }
}

keys_str!(BPlusTreeLeafPage);
