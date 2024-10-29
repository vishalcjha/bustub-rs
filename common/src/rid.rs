#![allow(dead_code)]

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct RID {
    pub page_id: usize,
    pub slot_num: u32,
}

impl RID {
    pub fn new(page_id: usize, slot_num: u32) -> Self {
        Self { page_id, slot_num }
    }
}
