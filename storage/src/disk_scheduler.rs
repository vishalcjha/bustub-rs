use std::{
    io,
    sync::mpsc::{channel, Sender},
    thread::{self, JoinHandle},
};

use crate::{disk_manager::DiskManager, disk_request::DiskRequest};

pub struct DiskScheduler {
    request_handler: JoinHandle<()>,
    request_submitter: Sender<DiskRequest>,
}

impl DiskScheduler {
    fn new(db_path: &str) -> DiskScheduler {
        let mut disk_manager = DiskManager::new(db_path).expect("Disk manager creation failed");
        let (tx, rx) = channel::<DiskRequest>();
        let request_handler = thread::spawn(move || {
            while let Ok(request) = rx.recv() {
                match request {
                    DiskRequest::Read {
                        page_id,
                        data_buf,
                        ack,
                    } => {
                        let res = disk_manager.read_page(page_id, data_buf);
                        let _ = ack.send(res);
                    }
                    DiskRequest::Write {
                        page_id,
                        data_buf,
                        ack,
                    } => {
                        let res = disk_manager.write_page(page_id, data_buf);
                        let _ = ack.send(res);
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
    pub(crate) fn schedule(&self, disk_request: DiskRequest) -> io::Result<()> {
        self.request_submitter.send(disk_request).map_err(|_| {
            io::Error::new(io::ErrorKind::NotConnected, "Failed to submit disk request")
        })?;
        Ok(())
    }
}
