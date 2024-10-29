use std::io;
pub(crate) mod disk;
pub(crate) mod page;

pub use disk::disk_request::DiskRequest;
pub use disk::disk_scheduler::DiskScheduler;
pub use disk::memory_manager::MemoryManager;
pub use page::page_guard::*;
pub use page::BPLUS_TREE_INTERNAL_PAGE_HEADER_SIZE;
pub use page::BPLUS_TREE_LEAF_PAGE_HEADER_SIZE;

pub use page::b_plus_tree_page::*;
pub use page::frame_header::*;
const PAGE_SIZE: usize = (4 * 1024) / std::mem::size_of::<u8>();

pub trait PageOperator: Send {
    fn write_page(&mut self, page_id: usize, data: &[u8; PAGE_SIZE]) -> io::Result<()>;
    fn read_page(&mut self, page_id: usize, data: &mut [u8; PAGE_SIZE]) -> io::Result<()>;
}
