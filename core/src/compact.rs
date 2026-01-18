use crossbeam_channel::{Receiver, Sender, unbounded};
use dashmap::DashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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

/*
Inspired by Go's GPM model LOL
*/

/*  It's a Multi-Lane Executor

                                  ┌─────────────────────────┐
                                  │   Dispatcher Thread     │
                                  │  (recv Task, dispatch)  │
                                  └───────────┬─────────────┘
                                              │
        ┌─────────────────────────────────────┼─────────────────────────────────────┐
        ▼                                     ▼                                     ▼
┌───────────────┐                    ┌───────────────┐                    ┌───────────────┐
│  Lane "A"     │                    │  Lane "B"     │                    │  Lane "C"     │
│ (Collection A)│                    │ (Collection B)│                    │ (Collection C)│
│               │                    │               │                    │               │
│  ┌─────────┐  │                    │  ┌─────────┐  │                    │  (Empty)      │
│  │ Task a1 │  │ <-- Worker 1       │  │ Task b1 │  │ <-- Worker 2       │               │
│  ├─────────┤  │     processing     │  ├─────────┤  │     processing     │               │
│  │ Task a2 │  │                    │  │ Task b2 │  │                    │               │
│  ├─────────┤  │                    │  └─────────┘  │                    │               │
│  │ Task a3 │  │                    │               │                    │               │
│  └─────────┘  │                    │               │                    │               │
└───────────────┘                    └───────────────┘                    └───────────────┘

                                     Worker Pool (N Workers)
                      ┌────────────────────────────────────────────────┐
                      │  Worker 1      Worker 2       Worker 3  ...    │
                      │  (Lane A)      (Lane B)        (idle, waiting) │
                      └────────────────────────────────────────────────┘
 */

pub struct Lane {
    pub task_queue: Mutex<VecDeque<CompactTask>>,
    pub is_processing: AtomicBool,
}

impl Lane {
    pub fn new() -> Self {
        Lane {
            task_queue: Mutex::new(VecDeque::with_capacity(10)),
            is_processing: AtomicBool::new(false),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Lane {
            task_queue: Mutex::new(VecDeque::with_capacity(capacity)),
            is_processing: AtomicBool::new(false),
        }
    }
}

impl Default for Lane {
    fn default() -> Self {
        Self::new()
    }
}

// NOTE: We should have a dynamic worker count, scale with the number of tasks

pub struct CompactionManager {
    path: PathBuf,
    lanes: Arc<DashMap<String, Lane>>,
    min_worker_count: usize,
    max_worker_count: usize,
}

impl CompactionManager {
    pub fn new(path: PathBuf) -> Self {
        let path = path.join("compact");
        CompactionManager {
            path: path,
            lanes: Arc::new(DashMap::new()),
            min_worker_count: 4,
            max_worker_count: 16,
        }
    }

    pub fn new_task_queue(&mut self, name: &str) {
        self.lanes.insert(name.to_string(), Lane::default());
    }

    pub fn spin_up_dispatcher(&self) -> Sender<CompactTask> {
        let (sx, rx): (Sender<CompactTask>, Receiver<CompactTask>) = unbounded();
        let lanes = self.lanes.clone();

        thread::spawn(move || {
            while let Ok(task) = rx.recv() {
                let lane = lanes
                    .entry(task.collection_name.clone())
                    .or_insert_with(|| Lane::default());

                lane.task_queue.lock().unwrap().push_back(task);
            }
        });
        sx
    }

    pub fn spin_up_workers(&self) {
        for _ in 0..self.min_worker_count {
            let lanes = self.lanes.clone();
            thread::spawn(move || {
                loop {
                    // All workers should check all lanes
                    // and pick one task to process (if the lane is not processing)
                    // if no task is found, sleep for a while

                    let mut task_found = false;

                    for lane in lanes.iter() {
                        if lane
                            .is_processing
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            let task = lane.task_queue.lock().unwrap().pop_front();
                            if let Some(task) = task {
                                task_found = true;
                                // write to disk
                            }
                            lane.is_processing.store(false, Ordering::SeqCst);
                        }

                        // NOTE: Just do it like this for now, but it is not efficient enough
                        // as it can cause starvation. The collection in the front always get processed first.
                        if task_found {
                            break;
                        }
                    }

                    if !task_found {
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            });
        }
    }
}
