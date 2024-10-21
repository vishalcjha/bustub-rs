use std::io;

use tokio::sync::oneshot;

use crate::PAGE_SIZE;

#[derive(Debug)]
pub enum DiskRequest {
    Read {
        page_id: usize,
        data_buf: &'static mut [u8; PAGE_SIZE],
        ack: tokio::sync::oneshot::Sender<io::Result<()>>,
    },
    Write {
        page_id: usize,
        data_buf: &'static [u8; PAGE_SIZE],
        ack: tokio::sync::oneshot::Sender<io::Result<()>>,
    },
}

impl DiskRequest {
    pub fn new_read(
        page_id: usize,
        data_buf: &'static mut [u8; PAGE_SIZE],
    ) -> (DiskRequest, oneshot::Receiver<io::Result<()>>) {
        let (tx, rx) = oneshot::channel();
        (
            DiskRequest::Read {
                page_id,
                data_buf,
                ack: tx,
            },
            rx,
        )
    }

    pub fn new_write(
        page_id: usize,
        data_buf: &'static [u8; PAGE_SIZE],
    ) -> (DiskRequest, oneshot::Receiver<io::Result<()>>) {
        let (tx, rx) = oneshot::channel();

        (
            DiskRequest::Write {
                page_id,
                data_buf,
                ack: tx,
            },
            rx,
        )
    }
}
