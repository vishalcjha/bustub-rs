use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::PAGE_SIZE;

pub(super) struct DiskManager {
    db_path: PathBuf,
    db_file: File,
}

impl DiskManager {
    pub(super) fn new(path: &str) -> io::Result<DiskManager> {
        let path_buf = PathBuf::from(path);
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        Ok(DiskManager {
            db_path: path_buf,
            db_file: file,
        })
    }

    pub(super) fn write_page(&mut self, page_id: usize, data: &[u8; PAGE_SIZE]) -> io::Result<()> {
        let beginning_offset = page_id * PAGE_SIZE;
        self.db_file
            .seek(SeekFrom::Start(beginning_offset as u64))?;
        self.db_file.write_all(data)?;
        self.db_file.flush()?;

        Ok(())
    }

    pub(super) fn read_page(
        &mut self,
        page_id: usize,
        data: &mut [u8; PAGE_SIZE],
    ) -> io::Result<()> {
        let beginning_offset = page_id * PAGE_SIZE;
        self.db_file
            .seek(SeekFrom::Start(beginning_offset as u64))?;
        self.db_file.read_exact(data)?;
        Ok(())
    }
}
