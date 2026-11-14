//! Partition thread custody, separate from protocol shutdown acknowledgements.
use alloc::{
    boxed::Box,
    vec::Vec,
};

use sitas_core::shard_runtime::ShardThread;
use spin::Mutex;

use crate::BrokerError;

pub(super) trait Worker: Send {
    fn is_finished(&self) -> bool;
    fn join(self: Box<Self>) -> Result<(), BrokerError>;
}

pub(super) struct Slot<J> {
    pub handle: Option<J>,
}
impl<J: ShardThread<()>> Worker for Slot<J> {
    fn is_finished(&self) -> bool {
        self.handle.as_ref().expect("published worker").is_finished()
    }

    fn join(mut self: Box<Self>) -> Result<(), BrokerError> {
        // Only terminal native observation, not an application acknowledgement,
        // permits consuming the backend join/result owner.
        self.handle.take().expect("published worker").join().map_err(|_| BrokerError::WorkerFailed)
    }
}

pub(super) struct Workers(pub Mutex<Vec<Box<dyn Worker>>>);
impl Workers {
    pub fn join_ready(&self) -> Result<bool, BrokerError> {
        loop {
            let ready = {
                let mut workers = self.0.lock();
                if workers.is_empty() {
                    return Ok(true);
                }
                workers
                    .iter()
                    .position(|worker| worker.is_finished())
                    .map(|index| workers.swap_remove(index))
            };
            match ready {
                Some(worker) => worker.join()?,
                None => return Ok(false),
            }
        }
    }

    pub fn retain(&mut self) {
        // Terminal fallback: keep the actual unjoined handles, never reconstruct
        // them from TIDs or report that dropping an observation ended a worker.
        core::mem::forget(core::mem::take(self.0.get_mut()));
    }
}
