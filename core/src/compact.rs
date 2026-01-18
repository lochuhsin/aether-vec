use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use dashmap::DashMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::memtable::MemTable;

#[derive(Clone)]
pub struct BackgroundContext {
    pub compact_task_sender: Sender<CompactTask>,
}

pub struct CompactTask {
    pub collection_name: String,
    pub seq_no: u64,
    pub memtable: Arc<dyn MemTable>,
}

pub struct CollectionStatus {}

pub struct CompactionManager {
    path: PathBuf,
    task_queues: Arc<DashMap<String, VecDeque<CompactTask>>>,
}

impl CompactionManager {
    pub fn new(path: PathBuf) -> Self {
        let path = path.join("compact");
        CompactionManager {
            path: path,
            task_queues: Arc::new(DashMap::new()),
        }
    }

    pub fn new_task_queue(&mut self, name: &str) {
        self.task_queues
            .insert(name.to_string(), VecDeque::with_capacity(10));
    }

    pub fn spin_up_dispatcher(&self) -> Sender<CompactTask> {
        let (sx, rx): (Sender<CompactTask>, Receiver<CompactTask>) = unbounded();
        let task_queue = self.task_queues.clone();

        thread::spawn(move || {
            while let Ok(task) = rx.recv() {
                let mut queue = task_queue.get_mut(&task.collection_name).unwrap();
                queue.push_back(task);
            }
        });
        sx
    }
}
