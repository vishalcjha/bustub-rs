#![allow(dead_code)]
use std::sync::{mpsc::Sender, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use tokio::sync::oneshot;

use crate::FrameHeader;

type DexTxSender = Sender<(usize, oneshot::Sender<()>)>;

pub struct ReadPageGuard {
    lock: Arc<RwLock<FrameHeader>>,
    read_guard: RwLockReadGuard<'static, FrameHeader>,
    dec_tx: DexTxSender,
}
pub struct WritePageGuard {
    lock: Arc<RwLock<FrameHeader>>,
    pub write_guard: RwLockWriteGuard<'static, FrameHeader>,
    dec_tx: DexTxSender,
}

pub fn read_page_guard(lock: Arc<RwLock<FrameHeader>>, dec_tx: DexTxSender) -> ReadPageGuard {
    let read_guard = unsafe {
        std::mem::transmute::<RwLockReadGuard<'_, FrameHeader>, RwLockReadGuard<'static, FrameHeader>>(
            lock.read().unwrap(),
        )
    };
    read_guard.incr_pin_count();
    ReadPageGuard {
        lock,
        read_guard,
        dec_tx,
    }
}

pub fn write_page_guard(lock: Arc<RwLock<FrameHeader>>, dec_tx: DexTxSender) -> WritePageGuard {
    let mut write_guard = unsafe {
        std::mem::transmute::<
            RwLockWriteGuard<'_, FrameHeader>,
            RwLockWriteGuard<'static, FrameHeader>,
        >(lock.write().unwrap())
    };
    write_guard.incr_pin_count();
    write_guard.set_dirty(true);
    WritePageGuard {
        lock,
        write_guard,
        dec_tx,
    }
}

impl Drop for ReadPageGuard {
    fn drop(&mut self) {
        self.read_guard.decr_pin_count();
        let (tx, rx) = oneshot::channel();
        self.dec_tx.send((self.read_guard.frame_id(), tx)).unwrap();
        rx.blocking_recv().unwrap();
    }
}

impl Drop for WritePageGuard {
    fn drop(&mut self) {
        self.write_guard.decr_pin_count();
        let (tx, rx) = oneshot::channel();
        self.dec_tx.send((self.write_guard.frame_id(), tx)).unwrap();
        rx.blocking_recv().unwrap();
    }
}
