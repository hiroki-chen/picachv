use lazy_static::lazy_static;
use picachv_error::PicachvResult;

/// Parameters for the monitor initialization.
pub struct InitParams {}

/// The definition of our policy monitor.
pub struct PicachvMonitor {
    pub(crate) ctx: Vec<()>,
}

impl PicachvMonitor {
    pub fn new() -> Self {
        PicachvMonitor { ctx: Vec::new() }
    }

    pub fn init(&mut self, params: InitParams) -> PicachvResult<()> {
        todo!()
    }
}

lazy_static! {
    pub static ref MONITOR: PicachvMonitor = PicachvMonitor::new();
}
