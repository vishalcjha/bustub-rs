#![allow(dead_code)]
use std::sync::{mpsc::Sender, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use tokio::sync::oneshot;

use crate::FrameHeader;

type DexTxSender = Sender<(usize, oneshot::Sender<()>)>;

pub struct ReadPageGuard {
    lock: Arc<RwLock<FrameHeader>>,
    read_guard: Option<RwLockReadGuard<'static, FrameHeader>>,
    dec_tx: DexTxSender,
}
pub struct WritePageGuard {
    lock: Arc<RwLock<FrameHeader>>,
    write_guard: Option<RwLockWriteGuard<'static, FrameHeader>>,
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
        read_guard: Some(read_guard),
        dec_tx,
    }
}

impl WritePageGuard {
    pub fn get_write_guard(&mut self) -> &mut RwLockWriteGuard<'static, FrameHeader> {
        self.write_guard.as_mut().unwrap()
    }
}

impl ReadPageGuard {
    pub fn get_read_guard(&self) -> &RwLockReadGuard<'static, FrameHeader> {
        self.read_guard.as_ref().unwrap()
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
        write_guard: Some(write_guard),
        dec_tx,
    }
}

impl Drop for ReadPageGuard {
    fn drop(&mut self) {
        let read_guard = self.read_guard.take().unwrap();
        read_guard.decr_pin_count();
        let (tx, rx) = oneshot::channel();
        self.dec_tx.send((read_guard.frame_id(), tx)).unwrap();
        // dropping this guard is important. Otherwise say a write thread for same frame,
        // holding bpm lock, will be held. And dec_tx also needs that lock.
        drop(read_guard);
        rx.blocking_recv().unwrap();
    }
}

impl Drop for WritePageGuard {
    fn drop(&mut self) {
        let write_guard = self.write_guard.take().unwrap();
        write_guard.decr_pin_count();
        let (tx, rx) = oneshot::channel();
        self.dec_tx.send((write_guard.frame_id(), tx)).unwrap();
        // dropping this guard is important. Otherwise say a reader thread for same frame,
        // holding bpm lock, will be held. And dec_tx also needs that lock.
        drop(write_guard);
        rx.blocking_recv().unwrap();
    }
}
