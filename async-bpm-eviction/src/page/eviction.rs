use super::{PageHandle, PageId, PageRef};
use crate::{
    bpm::BufferPoolManager,
    disk::{disk_manager::DiskManager, frame::Frame},
};
use async_channel::{Receiver, Sender};
use futures::stream::StreamExt;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

#[derive(Debug)]
pub struct Temperature {
    inner: AtomicU8,
}

#[derive(Debug)]
#[repr(u8)]
pub enum TemperatureState {
    Hot = 2,
    Cool = 1,
    Cold = 0,
}

impl Temperature {
    pub fn new(state: TemperatureState) -> Self {
        Self {
            inner: AtomicU8::new(state as u8),
        }
    }

    pub fn store(&self, state: TemperatureState, order: Ordering) {
        self.inner.store(state as u8, order);
    }

    pub fn load(&self, order: Ordering) -> TemperatureState {
        match self.inner.load(order) {
            0 => TemperatureState::Cold,
            1 => TemperatureState::Cool,
            2 => TemperatureState::Hot,
            _ => unreachable!("Had an invalid value inside the `Temperature` struct"),
        }
    }
}

#[derive(Debug)]
pub struct Evictor {
    pub(crate) free_frames: (Sender<Frame>, Receiver<Frame>),
    active_pages: (Sender<PageRef>, Receiver<PageRef>),
    bpm: Arc<BufferPoolManager>,
    disk_manager: Arc<DiskManager>,
}

impl Evictor {
    /// Attempt to grab a free page, otherwise evict a frame
    pub async fn get_free_frame(&self) -> Frame {
        match self.free_frames.1.recv().await {
            Ok(frame) => frame,
            Err(_) => unreachable!(),
        }
    }

    /// Adds to the set of active pages
    pub async fn page_make_active(&self, page: &PageRef) {
        self.active_pages
            .0
            .send(page.clone())
            .await
            .expect("Active pages channel closed unexpectedly");
    }

    /// Runs an eviction algorithm to free up pages
    pub async fn evict(&self) {
        // Collect all of the pages to evict
        let pages_to_evict: Vec<PageRef> = self
            .active_pages
            .1
            .clone()
            .take(64)
            .filter_map(|page| async {
                match page.eviction_state.load(Ordering::Acquire) {
                    TemperatureState::Cold => {
                        unreachable!("Found a Cold page in the active list of pages")
                    }
                    TemperatureState::Cool => Some(page),
                    TemperatureState::Hot => {
                        // If the page is Hot, we simply cool it and add it back to the queue
                        page.eviction_state
                            .store(TemperatureState::Cool, Ordering::Release);
                        self.page_make_active(&page).await;
                        None
                    }
                }
            })
            .collect()
            .await;

        // TODO actually batch write
        for page in pages_to_evict {
            let ph = PageHandle::new(
                page.clone(),
                self.bpm.clone(),
                self.disk_manager.create_handle(),
            );

            if let Err(_) = ph.try_evict().await {
                // If we cannot get the write lock immediately add it back to the queue
                self.page_make_active(&page).await
            }
        }
    }

    /// Each thread should spawn at least 1 of these tasks
    pub async fn evictor_task(&self) -> ! {
        loop {
            self.evict().await
        }
    }
}
