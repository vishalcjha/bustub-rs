#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub enum AccessType {
    Unknown,
    Lookup,
    Scan,
    Index,
}

/// Each not in LruKCache has K last access history.
/// It also has info about which frame.
struct LruNode {
    history: Vec<u64>,
    frame_id: usize,
    k: usize,
    evictable: bool,
}

impl LruNode {
    fn new(frame_id: usize, k: usize) -> Self {
        Self {
            history: Vec::with_capacity(k),
            frame_id,
            k,
            evictable: false,
        }
    }

    fn add_history(&mut self) {
        if self.history.len() >= self.k {
            self.history.pop();
        }
        let now = SystemTime::now();
        let since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
        let nanos = since_epoch.as_secs() * 1_000_000_000 + since_epoch.subsec_nanos() as u64;
        self.history.push(nanos);
    }
}

pub(super) struct LruKReplacer {
    size: usize,
    node_store: Arc<Mutex<HashMap<usize, LruNode>>>,
    k: usize,
    current_size: AtomicU32,
}

impl LruKReplacer {
    pub(super) fn new(size: usize, k: usize) -> Self {
        Self {
            size,
            node_store: Arc::default(),
            k,
            current_size: AtomicU32::new(0),
        }
    }

    /// Record the event that the given frame id is accessed at current timestamp.
    /// Create a new entry for access history if frame id has not been seen before.
    /// If frame id is invalid (ie. larger than replacer_size_), throw an exception.
    pub(super) fn record_access(&self, frame_id: usize, _access_type: Option<AccessType>) {
        self.panic_if_not_valid_frame_id(frame_id);
        let mut guard = self.node_store.lock().unwrap();
        if let Some(node) = guard.get_mut(&frame_id) {
            node.add_history();
            return;
        }

        let mut node = LruNode::new(frame_id, self.k);
        node.add_history();
        guard.insert(frame_id, node);
    }

    /// Number of evictable frame.
    pub(super) fn size(&self) -> usize {
        self.current_size.load(Ordering::SeqCst) as usize
    }

    /// If frame id is invalid, throw an exception or abort the process.
    /// Toggle whether a frame is evictable or non-evictable. This function also
    /// controls replacer's size. Note that size is equal to number of evictable entries.
    /// If a frame was previously evictable and is to be set to non-evictable, then size should
    /// decrement. If a frame was previously non-evictable and is to be set to evictable, then size should increment.
    ///
    ///  For other scenarios, this function should terminate without modifying anything.
    pub(super) fn set_evictable(&self, frame_id: usize, is_evictable: bool) {
        self.panic_if_not_valid_frame_id(frame_id);

        let mut guard = self.node_store.lock().unwrap();
        let Some(node) = guard.get_mut(&frame_id) else {
            return;
        };

        if node.evictable && !is_evictable {
            node.evictable = false;
            self.current_size.fetch_sub(1, Ordering::SeqCst);
        } else if !node.evictable && is_evictable {
            node.evictable = true;
            self.current_size.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Remove an evictable frame from replacer, along with its access history.
    /// This function should also decrement replacer's size if removal is successful.
    /// Note that this is different from evicting a frame, which always remove the frame
    ///  with largest backward k-distance. This function removes specified frame id, no matter what its backward k-distance is.
    ///
    /// If Remove is called on a non-evictable frame, throw an exception or abort the process.
    ///
    /// If specified frame is not found, directly return from this function.
    pub(super) fn remove(&self, frame_id: usize) {
        let mut guard = self.node_store.lock().unwrap();
        if let Some(_) = guard.remove(&frame_id) {
            self.current_size.fetch_sub(1, Ordering::SeqCst);
        }
    }

    ///Find the frame with largest backward k-distance and evict that frame. Only frames
    /// that are marked as 'evictable' are candidates for eviction.
    ///
    /// A frame with less than k historical references is given +inf as its backward k-distance.
    /// If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
    /// based on LRU.
    ///
    /// Successful eviction of a frame should decrement the size of replacer and remove the frame's access history.
    ///
    pub(super) fn evict(&self) -> Option<usize> {
        let mut guard = self.node_store.lock().unwrap();
        let mut less_than_k: Option<(usize, u64)> = None;
        let mut k_history: Option<(usize, u64)> = None;
        for (k, v) in guard.iter() {
            if !v.evictable {
                continue;
            }

            if v.history.len() < self.k {
                if less_than_k.map(|it| it.1.gt(&v.history[0])).unwrap_or(true) {
                    less_than_k = Some((k.clone(), v.history[0].clone()));
                }
            } else if less_than_k.is_none() {
                if k_history
                    .map(|it| it.1.gt(&v.history.last().unwrap()))
                    .unwrap_or(true)
                {
                    k_history = Some((k.clone(), v.history.last().unwrap().clone()));
                }
            }
        }
        if let Some(frame_id) = less_than_k
            .map(|it| it.0)
            .or_else(|| k_history.map(|it| it.0))
        {
            guard.remove(&frame_id);
            self.current_size.fetch_sub(1, Ordering::SeqCst);
            return Some(frame_id);
        }

        None
    }

    fn panic_if_not_valid_frame_id(&self, frame_id: usize) {
        if self.size < frame_id {
            panic!("Invalid frame id {}.", frame_id);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::lruk_replacer::LruKReplacer;

    #[test]
    fn sample_test() {
        let replacer = LruKReplacer::new(7, 2);
        for i in 1..7 {
            replacer.record_access(i, None);
        }

        // Add six frames to the replacer. We now have frames [1, 2, 3, 4, 5]. We set frame 6 as non-evictable.
        for i in 1..6 {
            replacer.set_evictable(i, true);
        }
        replacer.set_evictable(6, false);

        // The size of the replacer is the number of frames that can be evicted, _not_ the total number of frames entered.
        assert_eq!(5, replacer.size());

        // Record an access for frame 1. Now frame 1 has two accesses total.
        replacer.record_access(1, None);

        // Evict three pages from the replacer.
        // To break ties, we use LRU with respect to the oldest timestamp, or the least recently used frame.
        for i in 2..=4 {
            assert_eq!(i, replacer.evict().unwrap());
        }
        assert_eq!(2, replacer.size());

        // Now the replacer has the frames [5, 1].

        // Insert new frames [3, 4], and update the access history for 5. Now, the ordering is [3, 1, 5, 4].
        for i in [3, 4, 5, 4] {
            replacer.record_access(i, None);
        }
        replacer.set_evictable(3, true);
        replacer.set_evictable(4, true);
        assert_eq!(4, replacer.size());

        // Look for a frame to evict. We expect frame 3 to be evicted next.
        assert_eq!(3, replacer.evict().unwrap());
        assert_eq!(3, replacer.size());

        // Set 6 to be evictable. 6 Should be evicted next since it has the maximum backward k-distance.
        replacer.set_evictable(6, true);
        assert_eq!(4, replacer.size());
        assert_eq!(6, replacer.evict().unwrap());
        assert_eq!(3, replacer.size());

        // Mark frame 1 as non-evictable. We now have [5, 4].
        replacer.set_evictable(1, false);

        // We expect frame 5 to be evicted next.
        assert_eq!(2, replacer.size());
        assert_eq!(5, replacer.evict().unwrap());
        assert_eq!(1, replacer.size());

        // Update the access history for frame 1 and make it evictable. Now we have [4, 1].
        replacer.record_access(1, None);
        replacer.record_access(1, None);
        replacer.set_evictable(1, true);
        assert_eq!(2, replacer.size());

        // Evict the last two frames.
        assert_eq!(4, replacer.evict().unwrap());
        assert_eq!(1, replacer.size());
        assert_eq!(1, replacer.evict().unwrap());
        assert_eq!(0, replacer.size());

        // Insert frame 1 again and mark it as non-evictable.
        replacer.record_access(1, None);
        replacer.set_evictable(1, false);
        assert_eq!(0, replacer.size());

        // A failed eviction should not change the size of the replacer.
        assert_eq!(None, replacer.evict());

        // Mark frame 1 as evictable again and evict it.
        replacer.set_evictable(1, true);
        assert_eq!(1, replacer.size());
        assert_eq!(1, replacer.evict().unwrap());
        assert_eq!(0, replacer.size());

        // There is nothing left in the replacer, so make sure this doesn't do something strange.
        assert_eq!(None, replacer.evict());
        assert_eq!(0, replacer.size());

        // Make sure that setting a non-existent frame as evictable or non-evictable doesn't do something strange.
        replacer.set_evictable(6, false);
        replacer.set_evictable(6, true);
    }
}
