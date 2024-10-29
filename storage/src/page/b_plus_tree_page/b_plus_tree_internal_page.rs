use super::{BPlusTreeInternalHeader, SizeHelper};

///
/// Store `n` indexed keys and `n + 1` child pointers (page_id) within internal page.
/// Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
/// K(i) <= K < K(i+1).
/// NOTE: Since the number of keys does not equal to number of child pointers,
/// the first key in key_array_ always remains invalid. That is to say, any search / lookup
/// should ignore the first key.
///
/// Internal page format (keys are stored in increasing order):
///  ---------
/// | HEADER |
///  ---------
///  ------------------------------------------
/// | KEY(1)(INVALID) | KEY(2) | ... | KEY(n) |
///  ------------------------------------------
///  ---------------------------------------------
/// | PAGE_ID(1) | PAGE_ID(2) | ... | PAGE_ID(n) |
///  ---------------------------------------------
///
#[repr(C)]
pub struct BPlusTreeInternalPage<KeyType, ValueType> {
    keys: Vec<KeyType>,
    values: Vec<ValueType>,
}
pub const BPLUS_TREE_INTERNAL_PAGE_HEADER_SIZE: usize =
    std::mem::size_of::<BPlusTreeInternalHeader>();

impl<KeyType, ValueType> BPlusTreeInternalPage<KeyType, ValueType> {
    fn new() -> Self {
        Self {
            keys: Vec::with_capacity(SizeHelper::get_internal_page_slot_cnt::<
                BPLUS_TREE_INTERNAL_PAGE_HEADER_SIZE,
                KeyType,
                ValueType,
            >()),
            values: Vec::with_capacity(SizeHelper::get_internal_page_slot_cnt::<
                BPLUS_TREE_INTERNAL_PAGE_HEADER_SIZE,
                KeyType,
                ValueType,
            >()),
        }
    }
}

keys_str!(BPlusTreeInternalPage);
