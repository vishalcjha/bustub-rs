#![allow(dead_code, unused_variables)]
use std::{
    collections::{HashMap, HashSet},
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
    written_pages: HashSet<usize>,
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
            written_pages: HashSet::new(),
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

    // return none if there is not evitable frame.
    fn read_page(&self, page_id: usize) -> Option<storage::ReadPageGuard> {
        let mut protected = self.protected.lock().unwrap();
        let Some(frame_id) = self.get_frame_id(protected.deref_mut(), page_id) else {
            return None;
        };
        let frame = protected.frames[frame_id].clone();
        let pg_guard = storage::read_page_guard(frame, self.dec_tx.clone());
        protected.replacer.record_access(frame_id, None);
        protected.frame_pin_count.entry(frame_id).and_modify(|e| {
            e.fetch_add(1, Ordering::SeqCst);
        });

        Some(pg_guard)
    }

    // return none if there is not evitable frame.
    fn write_page(&self, page_id: usize) -> Option<storage::WritePageGuard> {
        let mut protected = self.protected.lock().unwrap();
        let Some(frame_id) = self.get_frame_id(protected.deref_mut(), page_id) else {
            return None;
        };
        let frame = protected.frames[frame_id].clone();
        let pg_guard = storage::write_page_guard(frame, self.dec_tx.clone());
        protected.replacer.record_access(frame_id, None);
        protected.frame_pin_count.entry(frame_id).and_modify(|e| {
            e.fetch_add(1, Ordering::SeqCst);
        });

        Some(pg_guard)
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
                let page_id = evicted_frame.get_page_id().unwrap();
                self.disk_scheduler
                    .schedule(storage::DiskRequest::Write {
                        page_id,
                        data_buf: evicted_frame.get_data_mut(),
                        ack: tx,
                    })
                    .unwrap();
                let data = rx.blocking_recv().unwrap().unwrap();
                protected.written_pages.insert(page_id);
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
        if protected.written_pages.contains(&page_id) {
            let (tx, rx) = oneshot::channel();
            self.disk_scheduler
                .schedule(storage::DiskRequest::Read {
                    page_id,
                    data_buf: assigned_frame.get_data_mut(),
                    ack: tx,
                })
                .unwrap();
            let data = rx.blocking_recv().unwrap().unwrap();
            assigned_frame.set_data(data);
        }
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
        let mut protected = self.protected.lock().unwrap();

        let Some(&frame_id) = protected.page_table.get(&page_id) else {
            protected.written_pages.remove(&page_id);
            return true;
        };

        if 0 != protected
            .frame_pin_count
            .get(&frame_id)
            .unwrap()
            .load(Ordering::SeqCst)
        {
            return false;
        }

        protected.written_pages.remove(&page_id);
        {
            let mut associated_frame = protected.frames[frame_id].write().unwrap();
            associated_frame.set_dirty(false);
            associated_frame.set_page_id(None);
        }

        protected.page_table.remove(&page_id);
        protected.free_frame_ids.push(frame_id);

        //TODO remove data from disk or memory

        true
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        thread,
        time::Duration,
    };

    use storage::MemoryManager;

    use super::BufferPoolManager;

    const FRAMES: usize = 10;
    const K_DIST: usize = 5;

    #[test]
    fn test_very_basic() {
        let disk_manager = MemoryManager::new(1000);
        let bpm = BufferPoolManager::new(FRAMES, K_DIST, Box::new(disk_manager));
        let pid = bpm.new_page_id();
        let hello_world = "hello world";

        // Check `WritePageGuard` basic functionality.
        {
            let mut guard = bpm.write_page(pid).unwrap();
            let data = guard.get_write_guard().get_writable_data();

            data[..hello_world.len()].copy_from_slice(hello_world.as_bytes());
        }

        // Check `ReadPageGuard` basic functionality.
        {
            assert_eq!(0, bpm.get_pin_count(pid).unwrap());
            let guard = bpm.read_page(pid).unwrap();
            let data = guard.get_read_guard().get_readable_data();

            assert_eq!(true, data[..hello_world.len()].eq(hello_world.as_bytes()));
        }

        // Check `ReadPageGuard` basic functionality (again).
        {
            assert_eq!(0, bpm.get_pin_count(pid).unwrap());
            let guard = bpm.read_page(pid).unwrap();
            let data = guard.get_read_guard().get_readable_data();

            assert_eq!(true, data[..hello_world.len()].eq(hello_world.as_bytes()));
        }
    }

    #[test]
    fn test_page_pin_easy_test() {
        let disk_manager = MemoryManager::new(1000);
        let bpm = BufferPoolManager::new(2, 5, Box::new(disk_manager));

        let page_id_0;
        let page_id_1;
        let page_0_data = "page0";
        let page_1_data = "page1";
        let page_0_updated = "page0updated";
        let page_1_updated = "page1updated";

        {
            page_id_0 = bpm.new_page_id();
            let mut page_0_guard = bpm.write_page(page_id_0).unwrap();
            let data = page_0_guard.get_write_guard().get_writable_data();
            data[..page_0_data.len()].copy_from_slice(page_0_data.as_bytes());

            page_id_1 = bpm.new_page_id();
            let mut page_1_guard = bpm.write_page(page_id_1).unwrap();
            let data = page_1_guard.get_write_guard().get_writable_data();
            data[..page_1_data.len()].copy_from_slice(page_1_data.as_bytes());

            assert_eq!(1, bpm.get_pin_count(page_id_0).unwrap());
            assert_eq!(1, bpm.get_pin_count(page_id_1).unwrap());

            // as there are two frames only, any new page should not be assigned frame.
            let temp1 = bpm.new_page_id();
            let temp_1_guard = bpm.write_page(temp1);
            assert_eq!(true, temp_1_guard.is_none());

            let temp2 = bpm.new_page_id();
            let temp_2_guard = bpm.read_page(temp2);
            assert_eq!(true, temp_2_guard.is_none());

            drop(page_0_guard);
            drop(page_1_guard);

            assert_eq!(0, bpm.get_pin_count(page_id_0).unwrap());
            assert_eq!(0, bpm.get_pin_count(page_id_1).unwrap());
        }

        {
            // now both should have frames as pervious frame pin count are 0 and thus evictable.
            let temp1 = bpm.new_page_id();
            let temp_1_guard = bpm.write_page(temp1);
            assert_eq!(true, temp_1_guard.is_some());

            let temp2 = bpm.new_page_id();
            let temp_2_guard = bpm.read_page(temp2);
            assert_eq!(true, temp_2_guard.is_some());
        }

        {
            let mut page_0_guard = bpm.write_page(page_id_0).unwrap();
            let data = page_0_guard.get_write_guard().get_writable_data();
            assert_eq!(true, data[..page_0_data.len()].eq(page_0_data.as_bytes()));
            data[..page_0_updated.len()].copy_from_slice(page_0_updated.as_bytes());

            let mut page_1_guard = bpm.write_page(page_id_1).unwrap();
            let data = page_1_guard.get_write_guard().get_writable_data();
            assert_eq!(true, data[..page_1_data.len()].eq(page_1_data.as_bytes()));
            data[..page_1_updated.len()].copy_from_slice(page_1_updated.as_bytes());

            assert_eq!(1, bpm.get_pin_count(page_id_0).unwrap());
            assert_eq!(1, bpm.get_pin_count(page_id_1).unwrap());
        }

        assert_eq!(0, bpm.get_pin_count(page_id_0).unwrap());
        assert_eq!(0, bpm.get_pin_count(page_id_1).unwrap());

        {
            let page_0_guard = bpm.read_page(page_id_0).unwrap();
            let data = page_0_guard.get_read_guard().get_readable_data();
            assert_eq!(
                true,
                data[..page_0_updated.len()].eq(page_0_updated.as_bytes())
            );

            let page_1_guard = bpm.read_page(page_id_1).unwrap();
            let data = page_1_guard.get_read_guard().get_readable_data();
            assert_eq!(
                true,
                data[..page_1_updated.len()].eq(page_1_updated.as_bytes())
            );

            assert_eq!(1, bpm.get_pin_count(page_id_0).unwrap());
            assert_eq!(1, bpm.get_pin_count(page_id_1).unwrap());
        }

        assert_eq!(0, bpm.get_pin_count(page_id_0).unwrap());
        assert_eq!(0, bpm.get_pin_count(page_id_1).unwrap());
    }

    #[test]
    fn page_pin_medium_test() {
        let disk_manager = MemoryManager::new(1000);
        let bpm = BufferPoolManager::new(FRAMES, K_DIST, Box::new(disk_manager));

        let hello = "Hello";
        let page_0 = bpm.new_page_id();
        let mut page_0_guard = bpm.write_page(page_0).unwrap();
        let data = page_0_guard.get_write_guard().get_writable_data();
        data[..hello.len()].copy_from_slice(hello.as_bytes());
        drop(page_0_guard);

        // Create a vector of unique pointers to page guards, which prevents the guards from getting destructed.
        let mut page_guards = Vec::new();

        // Scenario: We should be able to create new pages until we fill up the buffer pool.
        for i in 0..FRAMES {
            let page_id = bpm.new_page_id();
            let page_guard = bpm.write_page(page_id).unwrap();
            page_guards.push((page_id, page_guard));
        }

        // Scenario: All of the pin counts should be 1.
        for id_guard in page_guards.iter() {
            assert_eq!(1, bpm.get_pin_count(id_guard.0).unwrap());
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
        for i in 0..FRAMES {
            let page_id = bpm.new_page_id();
            let page_guard = bpm.write_page(page_id);
            assert_eq!(true, page_guard.is_none());
        }

        // Scenario: Drop the last 5 pages to unpin them.
        for i in 0..FRAMES / 2 {
            let pg_guard = page_guards.pop().unwrap();
            drop(pg_guard.1);
            assert_eq!(0, bpm.get_pin_count(pg_guard.0).unwrap());
        }

        // Scenario: All of the pin counts of the pages we haven't dropped yet should still be 1.
        for id_guard in page_guards.iter() {
            assert_eq!(1, bpm.get_pin_count(id_guard.0).unwrap());
        }

        // Scenario: After unpinning pages {6, 7, 8, 9, 10}, we should be able to create 4 new pages and bring them into
        // memory. Bringing those 4 pages into memory should evict the first 4 pages {6, 7, 8, 9,} because of LRU.
        for i in 0..(FRAMES / 2) - 1 {
            let page_id = bpm.new_page_id();
            let page_guard = bpm.write_page(page_id);
            assert_eq!(true, page_guard.is_some());
        }

        // Scenario: There should be one frame available, and we should be able to fetch the data we wrote a while ago.
        {
            let original_page = bpm.read_page(page_0).unwrap();
            let data = original_page.get_read_guard().get_readable_data();
            assert_eq!(true, data[..hello.len()].eq(hello.as_bytes()));
        }

        // Scenario: Once we unpin page 0 and then make a new page, all the buffer pages should now be pinned. Fetching page 0
        // again should fail.
        let last_pid = bpm.new_page_id();
        let last_page = bpm.read_page(last_pid).unwrap();
        let last_pid = bpm.new_page_id();
        let last_page = bpm.read_page(last_pid).unwrap();
        let last_pid = bpm.new_page_id();
        let last_page = bpm.read_page(last_pid).unwrap();
        let last_pid = bpm.new_page_id();
        let last_page = bpm.read_page(last_pid).unwrap();

        let fail = bpm.read_page(page_0);
        //assert_eq!(true, fail.is_none());
    }

    #[test]
    fn page_access_test() {
        let rounds = 50;
        let disk_manager = MemoryManager::new(1000);
        let bpm = BufferPoolManager::new(1, K_DIST, Box::new(disk_manager));

        let pid = bpm.new_page_id();
        println!("Spawning thread id {:?}", thread::current().id());

        thread::scope(|s| {
            let handler = s.spawn(|| {
                // The writer can keep writing to the same page.
                for i in 0..50 {
                    println!(
                        "Writer thread {:?} loop count {}",
                        thread::current().id(),
                        i
                    );
                    thread::sleep(Duration::from_millis(5));
                    let mut guard = bpm.write_page(pid).unwrap();
                    let to_write = i.to_string();
                    let data = guard.get_write_guard().get_writable_data();
                    data[..to_write.len()].copy_from_slice(to_write.as_bytes());
                }
            });

            s.spawn(|| {
                for i in 0..50 {
                    println!(
                        "Reader thread {:?} loop count {}",
                        thread::current().id(),
                        i
                    );
                    // Wait for a bit before taking the latch, allowing the writer to write some stuff.
                    thread::sleep(Duration::from_millis(10));

                    // While we are reading, nobody should be able to modify the data.
                    let guard = bpm.read_page(pid).unwrap();
                    // Save the data we observe.
                    let cloned_data = String::from_utf8(Vec::from(
                        guard.get_read_guard().get_readable_data().clone(),
                    ))
                    .unwrap();

                    // Sleep for a bit. If latching is working properly, nothing should be writing to the page.
                    thread::sleep(Duration::from_millis(10));
                    let cloned_data_again = String::from_utf8(Vec::from(
                        guard.get_read_guard().get_readable_data().clone(),
                    ))
                    .unwrap();
                    // Check that the data is unmodified.
                    assert_eq!(true, cloned_data.eq(&cloned_data_again));
                }
            });
        });
    }

    #[test]
    fn deadlock_test() {
        let disk_manager = MemoryManager::new(1000);
        let bpm = BufferPoolManager::new(FRAMES, K_DIST, Box::new(disk_manager));
        let page_id_0 = bpm.new_page_id();
        let page_id_1 = bpm.new_page_id();

        // A crude way of synchronizing threads, but works for this small case.
        let start = AtomicBool::new(false);

        thread::scope(|s| {
            s.spawn(|| {
                // Acknowledge that we can begin the test.
                start.store(true, Ordering::SeqCst);
                // Attempt to write to page 0.
                let guard = bpm.write_page(page_id_0);
            });

            // Wait for the other thread to begin before we start the test.
            loop {
                if start.load(Ordering::SeqCst) {
                    break;
                }
            }

            // Make the other thread wait for a bit.
            // This mimics the main thread doing some work while holding the write latch on page 0.
            thread::sleep(Duration::from_millis(1000));

            // If your latching mechanism is incorrect, the next line of code will deadlock.
            // Think about what might happen if you hold a certain "all-encompassing" latch for too long...

            // While holding page 0, take the latch on page 1.
            bpm.write_page(page_id_1);
        });
    }

    // #[test]
    // fn evictable_test() {
    //     let rounds = 1000;
    //     let num_threads = 8;

    //     let disk_manager = MemoryManager::new(1000);
    //     let bpm = BufferPoolManager::new(1, K_DIST, Box::new(disk_manager));

    //     thread::scope(|s| {
    //         for i in 0..rounds {
    //             let mutex = Mutex::new(());
    //             let cond = Condvar::new();

    //             let mut signal = false;
    //             let winner_pid = bpm.new_page_id();
    //             let looser_pid = bpm.new_page_id();

    //             for j in 0..num_threads {
    //                 s.spawn(|| {
    //                     let guard = mutex.lock().unwrap();

    //                     loop {
    //                         if !signal {
    //                             break;
    //                         }
    //                     }

    //                     cond.wait(guard);

    //                     let read_guard = bpm.read_page(winner_pid);
    //                     assert_eq!(false, bpm.read_page(looser_pid).is_some());
    //                 });
    //             }

    //             let guard = mutex.lock().unwrap();
    //             if i % 2 == 0 {
    //                 let read_guard = bpm.read_page(winner_pid);

    //                 signal = true;
    //             }
    //         }
    //     });
    // }
}
