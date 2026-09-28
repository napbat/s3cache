//! One-shot pause for a response that the real origin already returned.

use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub(super) struct ResponsePause {
    armed: AtomicBool,
    held: AtomicBool,
    released: AtomicBool,
}

impl ResponsePause {
    pub(super) fn arm(&self) {
        assert!(
            !self.armed.swap(true, Ordering::SeqCst),
            "response pause already armed"
        );
        self.held.store(false, Ordering::SeqCst);
        self.released.store(false, Ordering::SeqCst);
    }

    pub(super) fn claim(&self) -> bool {
        self.armed
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub(super) async fn hold(&self) {
        self.held.store(true, Ordering::SeqCst);
        while !self.released.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    pub(super) async fn wait_held(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while !self.held.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the origin response was held");
    }

    pub(super) fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
    }
}
