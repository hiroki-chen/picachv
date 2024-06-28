//! A simple thread pool implementation for spawning rayon threads.

use std::num::NonZeroUsize;
use std::sync::LazyLock;
use std::thread::available_parallelism;

use rayon::{ThreadPool, ThreadPoolBuilder};

/// The global thread pool.
pub static THREAD_POOL: LazyLock<ThreadPool> = LazyLock::new(|| {
    let thread_name = "picachv";

    ThreadPoolBuilder::new()
        .num_threads(
            available_parallelism()
                .unwrap_or(NonZeroUsize::new(1).unwrap())
                .get(),
        )
        .thread_name(move |i| format!("{}-{}", thread_name, i))
        .build()
        .unwrap()
});
