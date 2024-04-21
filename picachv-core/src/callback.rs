use picachv_error::PicachvResult;

pub const BUF_SIZE: usize = 1 << 16;

pub trait Callable: Send + Sync {
    fn call(&self) -> PicachvResult<Vec<u8>>;
    fn box_clone(&self) -> Box<dyn Callable>;
}

/// The caller of the callback function.
pub struct Caller {
    pub(crate) cb: Box<dyn Callable>,
}

impl Clone for Caller {
    fn clone(&self) -> Self {
        Self {
            cb: self.cb.box_clone(),
        }
    }
}

impl Caller {
    pub fn new(f: impl Callable + 'static) -> Self {
        Self { cb: Box::new(f) }
    }

    pub fn call(&self) -> PicachvResult<Vec<u8>> {
        self.cb.call()
    }
}
