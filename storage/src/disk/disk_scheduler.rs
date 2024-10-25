#![allow(dead_code)]
use std::{
    io,
    sync::mpsc::{channel, Sender},
    thread::{self, JoinHandle},
};

use crate::PageOperator;

use super::disk_request::DiskRequest;

pub struct DiskScheduler {
    request_handler: JoinHandle<()>,
    request_submitter: Sender<DiskRequest>,
}

impl DiskScheduler {
    pub fn new(_db_path: &str, page_operator: Box<dyn PageOperator>) -> DiskScheduler {
        let mut page_operator = page_operator;
        //let mut disk_manager = DiskManager::new(db_path).expect("Disk manager creation failed");
        let (tx, rx) = channel::<DiskRequest>();
        let request_handler = thread::spawn(move || {
            while let Ok(request) = rx.recv() {
                match request {
                    DiskRequest::Read {
                        page_id,
                        data_buf,
                        ack,
                    } => {
                        let mut data_buf = data_buf;
                        let res = page_operator.read_page(page_id, &mut data_buf);
                        let _ = ack.send(res.map(|_| data_buf));
                    }
                    DiskRequest::Write {
                        page_id,
                        data_buf,
                        ack,
                    } => {
                        let res = page_operator.write_page(page_id, &data_buf);
                        let _ = ack.send(res.map(|_| data_buf));
                    }
                }
            }
        });

        DiskScheduler {
            request_handler,
            request_submitter: tx,
        }
    }
}

impl DiskScheduler {
    pub fn schedule(&self, disk_request: DiskRequest) -> io::Result<()> {
        self.request_submitter.send(disk_request).map_err(|_| {
            io::Error::new(io::ErrorKind::NotConnected, "Failed to submit disk request")
        })?;
        Ok(())
    }
}
