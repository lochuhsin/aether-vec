use crate::SSTEvent;
use crate::compact::CompactTask;
use crossbeam_channel::{Receiver, Sender};

#[derive(Clone)]
pub struct BackgroundContext {
    pub compact_task_sender: Sender<CompactTask>,
}
