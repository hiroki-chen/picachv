const BUF_SIZE: usize = 1 << 16;

/// The callback function written in C so it remains opaque to the Rust code.
pub type Callback = unsafe extern "C" fn(affected_rows: *mut u8, affected_rows_len: *mut usize);

#[derive(Clone)]
pub struct Caller {
    cb: Box<Callback>,
}

impl Caller {
    pub fn new(f: Callback) -> Self {
        Self { cb: Box::new(f) }
    }

    pub fn call(&self) -> Vec<u8> {
        let mut len = 0usize;
        let mut ar = vec![0u8; BUF_SIZE];

        unsafe {
            (self.cb)(ar.as_mut_ptr(), &mut len);
        }
        ar[..len].to_vec()
    }
}
