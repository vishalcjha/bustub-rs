#![allow(dead_code)]
use std::io;

use tokio::sync::oneshot;

use crate::PAGE_SIZE;
type BoxedData = Box<[u8; PAGE_SIZE]>;
#[derive(Debug)]
pub enum DiskRequest {
    Read {
        page_id: usize,
        data_buf: BoxedData,
        ack: tokio::sync::oneshot::Sender<io::Result<BoxedData>>,
    },
    Write {
        page_id: usize,
        data_buf: BoxedData,
        ack: tokio::sync::oneshot::Sender<io::Result<BoxedData>>,
    },
}

impl DiskRequest {
    pub fn new_read(
        page_id: usize,
        data_buf: BoxedData,
    ) -> (DiskRequest, oneshot::Receiver<io::Result<BoxedData>>) {
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
        data_buf: BoxedData,
    ) -> (DiskRequest, oneshot::Receiver<io::Result<BoxedData>>) {
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
