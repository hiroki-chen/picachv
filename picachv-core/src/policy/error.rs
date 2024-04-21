/// A simple error type for describing why the data analysis may fail.
pub struct PolicyErrorReason {
    /// The location where the error occurs.
    pub(crate) loc: (usize, usize),
}
