#![allow(dead_code, unused_variables)]
use std::{
    collections::HashMap,
    ops::DerefMut,
    sync::{
        atomic::{AtomicU16, AtomicUsize, Ordering},
        mpsc::{channel, Sender},
        Arc, Mutex, RwLock,
    },
    thread, usize,
};

use storage::{DiskScheduler, FrameHeader, PageOperator};
use tokio::sync::oneshot;

use crate::lruk_replacer::LruKReplacer;
type DecTxSender = Sender<(usize, oneshot::Sender<()>)>;

struct Protected {
    frames: Vec<Arc<RwLock<FrameHeader>>>,
    free_frame_ids: Vec<usize>,
    // page_id to frame_id
    page_table: HashMap<usize, usize>,

    // this is just for test purpose. As frames may be write lock, we need a proxy way to get this.
    frame_pin_count: HashMap<usize, AtomicU16>,
    replacer: LruKReplacer,
}
struct BufferPoolManager {
    num_frames: usize,
    disk_scheduler: DiskScheduler,
    next_page_id: AtomicUsize,
    protected: Arc<Mutex<Protected>>,

    dec_tx: DecTxSender,
}

impl BufferPoolManager {
    fn new(num_frames: usize, k_dist: usize, page_operator: Box<dyn PageOperator>) -> Self {
        let disk_scheduler = DiskScheduler::new("my_db", page_operator);
        let frame_pin_count = (0..num_frames).map(|i| (i, AtomicU16::default())).collect();
        let frames = (0..num_frames)
            .into_iter()
            .map(|i| Arc::new(RwLock::new(FrameHeader::new(i))))
            .collect();

        let protected = Arc::new(Mutex::new(Protected {
            frames,
            free_frame_ids: (0..num_frames).collect(),
            page_table: HashMap::with_capacity(num_frames),
            frame_pin_count,
            replacer: LruKReplacer::new(num_frames, k_dist),
        }));
        let cloned_protected = protected.clone();
        let (tx, rx) = channel::<(usize, oneshot::Sender<()>)>();

        thread::spawn(move || loop {
            let (dec_frame_id, dec_sender) = rx.recv().unwrap();
            let guard = cloned_protected.lock().unwrap();
            guard
                .frame_pin_count
                .get(&dec_frame_id)
                .unwrap()
                .fetch_sub(1, Ordering::SeqCst);
            if let Some(count) = guard.frame_pin_count.get(&dec_frame_id) {
                if count.load(Ordering::SeqCst) == 0 {
                    guard.replacer.set_evictable(dec_frame_id, true);
                }
            }
            dec_sender.send(()).unwrap();
        });

        Self {
            num_frames,
            disk_scheduler,
            next_page_id: AtomicUsize::default(),
            protected,
            dec_tx: tx,
        }
    }

    fn new_page_id(&self) -> usize {
        self.next_page_id.fetch_add(1, Ordering::SeqCst)
    }

    fn read_page(&self, page_id: usize) -> storage::ReadPageGuard {
        let mut protected = self.protected.lock().unwrap();
        let Some(frame_id) = self.get_frame_id(protected.deref_mut(), page_id) else {
            panic!("Trying to read page not in page table");
        };
        let frame = protected.frames[frame_id].clone();
        let pg_guard = storage::read_page_guard(frame, self.dec_tx.clone());
        protected.replacer.record_access(frame_id, None);
        protected.frame_pin_count.entry(frame_id).and_modify(|e| {
            e.fetch_add(1, Ordering::SeqCst);
        });

        pg_guard
    }

    fn write_page(&self, page_id: usize) -> storage::WritePageGuard {
        let mut protected = self.protected.lock().unwrap();
        let Some(frame_id) = self.get_frame_id(protected.deref_mut(), page_id) else {
            panic!("Trying to read page not in page table");
        };
        let frame = protected.frames[frame_id].clone();
        let pg_guard = storage::write_page_guard(frame, self.dec_tx.clone());
        protected.replacer.record_access(frame_id, None);
        protected.frame_pin_count.entry(frame_id).and_modify(|e| {
            e.fetch_add(1, Ordering::SeqCst);
        });

        pg_guard
    }

    fn flush_page(&self, page_id: usize) {}
    fn flush_all_pages(&self) {}

    // this is internal info and only required for testing.
    // we will use proxy for frame count and should not be used else where.
    fn get_pin_count(&self, page_id: usize) -> Option<u16> {
        let protected = self.protected.lock().unwrap();
        let Some(frame_id) = protected.page_table.get(&page_id) else {
            return None;
        };
        protected
            .frame_pin_count
            .get(frame_id)
            .map(|it| it.load(Ordering::SeqCst))
    }

    // this is only for internal use. It assumes lock is acquired on protected data.
    fn get_frame_id(&self, protected: &mut Protected, page_id: usize) -> Option<usize> {
        if let Some(&frame_id) = protected.page_table.get(&page_id) {
            return Some(frame_id);
        }
        if protected.free_frame_ids.is_empty() {
            let Some(evicted_frame_id) = protected.replacer.evict() else {
                return None;
            };

            let mut evicted_frame = protected.frames[evicted_frame_id].write().unwrap();
            if evicted_frame.is_dirty() {
                let (tx, rx) = oneshot::channel();
                self.disk_scheduler
                    .schedule(storage::DiskRequest::Write {
                        page_id: evicted_frame.get_page_id().unwrap(),
                        data_buf: evicted_frame.get_data_mut(),
                        ack: tx,
                    })
                    .unwrap();
                let data = rx.blocking_recv().unwrap().unwrap();
                evicted_frame.set_data(data);
                evicted_frame.set_dirty(false);
                evicted_frame.set_page_id(None);
            }

            protected.free_frame_ids.push(evicted_frame_id);
        }

        let mut assigned_frame = protected.frames[protected.free_frame_ids.pop().unwrap()]
            .write()
            .unwrap();
        assigned_frame.set_page_id(Some(page_id));
        protected
            .page_table
            .insert(page_id, assigned_frame.frame_id());
        Some(assigned_frame.frame_id())
    }

    /// Removes a page from the database, both on disk and in memory.
    /// If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
    /// removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
    ///
    /// Ideally, we would want to ensure that all space on disk is used efficiently. That would mean the space that deleted
    /// pages on disk used to occupy should somehow be made available to new pages allocated by `NewPage`. But for later.
    ///
    /// `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
    fn delete_page(&self, page_id: usize) -> bool {
        // let mut protected = self.protected.write().unwrap();
        // let Some(&frame_id) = protected.page_table.get(&page_id) else {
        //     return true;
        // };

        // let frame = protected.frames.get_mut(frame_id).unwrap();

        todo!()
    }
}

#[cfg(test)]
mod test {
    use storage::MemoryManager;

    use super::BufferPoolManager;

    const FRAMES: usize = 10;
    const K_DIST: usize = 5;

    #[test]
    fn test_very_basic() {
        let disk_manager = MemoryManager::new(1000);
        let bpm = BufferPoolManager::new(FRAMES, K_DIST, Box::new(disk_manager));
        let pid = bpm.new_page_id();
        {
            let mut guard = bpm.write_page(pid);
            let mut data = guard.write_guard.get_data_mut();
            let data_to_write = "hello world".as_bytes();

            data[..data_to_write.len()].copy_from_slice("hello world".as_bytes());
        }

        assert_eq!(0, bpm.get_pin_count(pid).unwrap());
    }
}
