#[macro_export]
macro_rules! rwlock_unlock {
    ($lock:expr, $how:ident) => {
        match $lock.$how() {
            Ok(guard) => guard,
            Err(err) => return Err(picachv_error::PicachvError::ComputeError(
                err.to_string().into(),
            )),
        }
    };
}
