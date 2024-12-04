//! Profiler implementation

use std::borrow::Cow;
use std::sync::{Arc, LazyLock, RwLock};
use std::time::{Duration, SystemTime};

use ahash::{HashMap, HashMapExt};
use serde::{Deserialize, Serialize};

pub type Tick = (u128, u128);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stat<'a> {
    /// The name of this stat
    pub name: Cow<'a, str>,
    pub tick: Vec<Tick>,
}

/// A simple Rust profiler for collecting more accurate information.
pub struct PicachvProfiler<'a> {
    /// The stats collected by the profiler.
    ///
    /// The hashmap key is the group name of the stats, and the value is the stat itself.
    /// A stat contains a vector of start and end time of the profiling.
    pub stats: Arc<RwLock<HashMap<Cow<'a, str>, Stat<'a>>>>,
}

impl<'a> Default for PicachvProfiler<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> PicachvProfiler<'a> {
    pub fn new() -> Self {
        PicachvProfiler {
            stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn dump(&self) -> Vec<(Cow<'a, str>, Duration)> {
        let lock: std::sync::RwLockReadGuard<HashMap<Cow<'a, str>, Stat<'a>>> =
            self.stats.read().unwrap();
        lock.iter()
            .map(|(name, stat)| {
                let mut duration = Duration::from_micros(0);
                for &(start, end) in &stat.tick {
                    duration += Duration::from_micros((end - start) as _);
                }
                (name.clone(), duration)
            })
            .collect()
    }

    pub fn dump_raw(&self) -> Vec<(Cow<'a, str>, Vec<Duration>)> {
        self.stats
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.tick
                        .iter()
                        .map(|(start, end)| Duration::from_micros(*end as u64 - *start as u64))
                        .collect::<Vec<Duration>>(),
                )
            })
            .collect()
    }

    /// Profile a function call.
    pub fn profile<T, F: FnOnce() -> T>(&self, func: F, name: Cow<'static, str>) -> T {
        let start = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let result = func();
        let end = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let mut lock = self.stats.write().unwrap();
        match lock.get_mut(&name) {
            Some(stat) => {
                stat.tick.push((start, end));
            },
            None => {
                lock.insert(
                    name.clone(),
                    Stat {
                        name,
                        tick: vec![(start, end)],
                    },
                );
            },
        }

        result
    }
}

pub static PROFILER: LazyLock<PicachvProfiler> = LazyLock::new(PicachvProfiler::new);
