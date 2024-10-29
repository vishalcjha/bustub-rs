#![allow(dead_code)]
use std::sync::atomic::{AtomicU16, Ordering};

use crate::PAGE_SIZE;

type BoxedData = Box<[u8; PAGE_SIZE]>;

pub struct FrameHeader {
    frame_id: usize,
    page_id: Option<usize>,
    // this indicates how many operations are using this frame.
    // A page associated with non zero pin_count should not be evicted.
    pin_count: AtomicU16,
    is_dirty: bool,
    data: Option<BoxedData>,
}

impl FrameHeader {
    pub fn new(frame_id: usize) -> Self {
        Self {
            frame_id,
            page_id: None,
            pin_count: AtomicU16::default(),
            is_dirty: false,
            data: Some(Box::new([0u8; PAGE_SIZE])),
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    pub fn frame_id(&self) -> usize {
        self.frame_id
    }

    pub fn incr_pin_count(&self) -> u16 {
        self.pin_count.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn decr_pin_count(&self) -> u16 {
        self.pin_count.fetch_sub(1, Ordering::SeqCst) - 1
    }

    pub fn set_page_id(&mut self, page_id: Option<usize>) {
        self.page_id = page_id;
    }

    pub fn get_page_id(&self) -> Option<usize> {
        self.page_id
    }

    // this is only to be used at time for flush. As data needs to be transferred across thread.
    pub fn get_data_mut(&mut self) -> Box<[u8; PAGE_SIZE]> {
        self.data.take().unwrap()
    }

    pub fn get_writeable_data(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.data.as_deref_mut().unwrap()
    }

    pub fn get_writeable_data_as<T>(&mut self) -> &mut T {
        todo!()
    }

    pub fn get_readable_data(&self) -> &[u8; PAGE_SIZE] {
        self.data.as_deref().unwrap()
    }

    pub fn get_readable_data_as<T>(&self) -> &T {
        todo!()
    }

    pub fn set_data(&mut self, data: BoxedData) {
        self.data = Some(data);
    }
}
