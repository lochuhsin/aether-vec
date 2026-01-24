use crate::SSTEvent;
use crossbeam_channel::{Receiver, Sender, unbounded};
use dashmap::DashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::memtable::MemTable;
use crate::sst::SSTManager;

const DEFAULT_SST_LAYER: u64 = 0;

pub struct CompactTask {
    pub collection_name: String,
    pub seq_no: u64,
    pub layer: u64,
    pub memtable: Arc<dyn MemTable>,
}

impl CompactTask {
    pub fn new_default_layer(
        collection_name: String,
        seq_no: u64,
        memtable: Arc<dyn MemTable>,
    ) -> Self {
        Self {
            collection_name,
            seq_no,
            layer: DEFAULT_SST_LAYER,
            memtable,
        }
    }
}

/*
Inspired by Go's GPM model

Multi-Lane Executor (Found the name on internet ^ ^)

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
    lanes: Arc<DashMap<String, Lane>>,
    min_worker_count: usize,
    max_worker_count: usize,
    default_lane_capacity: usize,
    sst_manager: Arc<SSTManager>,
    sst_event_sender: Sender<SSTEvent>,
}

impl CompactionManager {
    pub fn new(path: PathBuf, sst_event_sender: Sender<SSTEvent>) -> Self {
        CompactionManager {
            lanes: Arc::new(DashMap::new()),
            min_worker_count: 4,
            max_worker_count: 16,
            default_lane_capacity: 50,
            sst_manager: Arc::new(SSTManager::new(path)),
            sst_event_sender: sst_event_sender,
        }
    }

    pub fn spin_up_dispatcher(&self) -> Sender<CompactTask> {
        let (sx, rx): (Sender<CompactTask>, Receiver<CompactTask>) = unbounded();
        let lanes = self.lanes.clone();
        let default_lane_capacity = self.default_lane_capacity;

        thread::spawn(move || {
            while let Ok(task) = rx.recv() {
                let lane = lanes
                    .entry(task.collection_name.clone())
                    .or_insert_with(|| Lane::with_capacity(default_lane_capacity));

                lane.task_queue.lock().unwrap().push_back(task);
            }
        });
        sx
    }

    pub fn spin_up_workers(&self) {
        for _ in 0..self.min_worker_count {
            let lanes = self.lanes.clone();
            let sst_manager = self.sst_manager.clone();
            let sst_event_sender = self.sst_event_sender.clone();
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
                            // TODO: Use a RAII Guard (LaneProcessingGuard) here to ensure `is_processing` is reset to false
                            // even if a panic occurs in the critical section (e.g. during disk write).
                            // Currently, if a panic happens, the flag remains true and the lane becomes dead.
                            let task_option = match lane.task_queue.lock() {
                                Ok(mut queue) => queue.pop_front(),
                                Err(_) => {
                                    eprintln!("WARN: task_queue lock was poisoned, recovering...");
                                    None
                                }
                            };

                            // TODO: Handle when sst failed to write to disk or something
                            if let Some(task) = task_option {
                                task_found = true;

                                let sst_metadata = sst_manager.write_memtable(
                                    task.collection_name.as_str(),
                                    task.seq_no,
                                    task.layer,
                                    task.memtable.as_ref(),
                                );

                                match sst_metadata {
                                    Ok(metadata) => {
                                        sst_event_sender.send(SSTEvent { metadata }).unwrap();
                                    }
                                    Err(e) => {
                                        panic!(
                                            "Not implement yet, Need to do this properly, we can add status to SSTEvent and determine by database level: {}",
                                            e
                                        );
                                    }
                                }
                            }
                            lane.is_processing.store(false, Ordering::SeqCst);
                        }

                        // TODO: Implement Round-Robin Fairness Strategy
                        // Current linear scan (from start to end) causes starvation for lanes at the end of the map.
                        // Workers should maintain a `current_offset` and start scanning from there, incrementing it after each check.
                        // offset = (offset + 1) % distinct_lanes_count
                        if task_found {
                            break;
                        }
                    }

                    if !task_found {
                        // TODO: This is inefficient as it consumes CPU cycles even when there is no work.
                        // A more efficient, event-driven approach would be to use a notification mechanism
                        // (like a condition variable, or another crossbeam-channel)
                        // to wake up idle workers only when new tasks are available.
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            });
        }
    }
}
