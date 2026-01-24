use crate::compact::CompactTask;
use crossbeam_channel::Sender;

#[derive(Clone)]
pub struct BackgroundContext {
    pub compact_task_sender: Sender<CompactTask>,
}
