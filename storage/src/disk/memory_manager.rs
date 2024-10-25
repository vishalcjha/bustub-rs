#![allow(dead_code)]
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use crate::{PageOperator, PAGE_SIZE};

pub struct MemoryManager {
    page_capacity: usize,
    memory: Cursor<Vec<u8>>,
}

impl MemoryManager {
    pub fn new(page_capacity: usize) -> Self {
        let memory = vec![0u8; page_capacity * PAGE_SIZE];
        Self {
            page_capacity,
            memory: Cursor::new(memory),
        }
    }

    fn assert_page_bound(&self, page_id: usize) {
        if page_id >= self.page_capacity {
            panic!(
                "Memory page manager can only full fill page_id below {} but requested {}",
                self.page_capacity, page_id
            );
        }
    }
}

impl PageOperator for MemoryManager {
    fn write_page(&mut self, page_id: usize, data: &[u8; crate::PAGE_SIZE]) -> std::io::Result<()> {
        self.assert_page_bound(page_id);
        let pos = page_id * PAGE_SIZE;
        self.memory.seek(SeekFrom::Start(pos as u64))?;
        self.memory.write_all(data)?;
        Ok(())
    }

    fn read_page(
        &mut self,
        page_id: usize,
        data: &mut [u8; crate::PAGE_SIZE],
    ) -> std::io::Result<()> {
        self.assert_page_bound(page_id);
        let pos = page_id * PAGE_SIZE;
        self.memory.seek(SeekFrom::Start(pos as u64))?;
        self.memory.read_exact(data)?;
        Ok(())
    }
}
