use std::borrow::Cow;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::{env, io};

#[derive(Debug)]
pub enum ErrorState {
    NotYetEncountered { err: PicachvError },
    AlreadyEncountered { prev_err_msg: String },
}

impl fmt::Display for ErrorState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorState::NotYetEncountered { err } => write!(f, "NotYetEncountered({err})")?,
            ErrorState::AlreadyEncountered { prev_err_msg } => {
                write!(f, "AlreadyEncountered({prev_err_msg})")?
            },
        };

        Ok(())
    }
}

#[derive(Clone)]
pub struct ErrorStateSync(Arc<Mutex<ErrorState>>);

impl std::ops::Deref for ErrorStateSync {
    type Target = Arc<Mutex<ErrorState>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for ErrorStateSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ErrorStateSync({})", &*self.0.lock().unwrap())
    }
}

impl ErrorStateSync {
    pub fn take(&self) -> PicachvError {
        let mut curr_err = self.0.lock().unwrap();

        match &*curr_err {
            ErrorState::NotYetEncountered { err: polars_err } => {
                // Need to finish using `polars_err` here so that NLL considers `err` dropped
                let prev_err_msg = polars_err.to_string();
                // Place AlreadyEncountered in `self` for future users of `self`
                let prev_err = std::mem::replace(
                    &mut *curr_err,
                    ErrorState::AlreadyEncountered { prev_err_msg },
                );
                // Since we're in this branch, we know err was a NotYetEncountered
                match prev_err {
                    ErrorState::NotYetEncountered { err } => err,
                    ErrorState::AlreadyEncountered { .. } => unreachable!(),
                }
            },
            ErrorState::AlreadyEncountered { prev_err_msg } => {
                picachv_err!(
                    ComputeError: "LogicalPlan already failed with error: '{}'", prev_err_msg,
                )
            },
        }
    }
}

impl From<PicachvError> for ErrorStateSync {
    fn from(err: PicachvError) -> Self {
        Self(Arc::new(Mutex::new(ErrorState::NotYetEncountered { err })))
    }
}

#[derive(Debug)]
pub struct ErrString(Cow<'static, str>);

impl<T> From<T> for ErrString
where
    T: Into<Cow<'static, str>>,
{
    fn from(msg: T) -> Self {
        if env::var("PANIC_ON_ERR").as_deref().unwrap_or("") == "1" {
            panic!("{}", msg.into())
        } else {
            ErrString(msg.into())
        }
    }
}

impl AsRef<str> for ErrString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for ErrString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for ErrString {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PicachvError {
    #[error("already done: {0}")]
    Already(ErrString),
    #[error("not found: {0}")]
    ColumnNotFound(ErrString),
    #[error("{0}")]
    ComputeError(ErrString),
    #[error("duplicate: {0}")]
    Duplicate(ErrString),
    #[error("invalid operation: {0}")]
    InvalidOperation(ErrString),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("no data: {0}")]
    NoData(ErrString),
    #[error("{0}")]
    OutOfBounds(ErrString),
    #[error("Privacy policy violation: {0}")]
    PrivacyError(ErrString),
    #[error("field not found: {0}")]
    SchemaFieldNotFound(ErrString),
    #[error("data types don't match: {0}")]
    SchemaMismatch(ErrString),
    #[error("lengths don't match: {0}")]
    ShapeMismatch(ErrString),
    #[error("string caches don't match: {0}")]
    StringCacheMismatch(ErrString),
    #[error("field not found: {0}")]
    StructFieldNotFound(ErrString),
}

pub type PicachvResult<T> = Result<T, PicachvError>;

impl PicachvError {
    pub fn wrap_msg(&self, func: &dyn Fn(&str) -> String) -> Self {
        use PicachvError::*;
        match self {
            Already(msg) => Already(func(msg).into()),
            ColumnNotFound(msg) => ColumnNotFound(func(msg).into()),
            ComputeError(msg) => ComputeError(func(msg).into()),
            Duplicate(msg) => Duplicate(func(msg).into()),
            InvalidOperation(msg) => InvalidOperation(func(msg).into()),
            Io(err) => ComputeError(func(&format!("IO: {err}")).into()),
            NoData(msg) => NoData(func(msg).into()),
            OutOfBounds(msg) => OutOfBounds(func(msg).into()),
            PrivacyError(msg) => PrivacyError(func(msg).into()),
            SchemaFieldNotFound(msg) => SchemaFieldNotFound(func(msg).into()),
            SchemaMismatch(msg) => SchemaMismatch(func(msg).into()),
            ShapeMismatch(msg) => ShapeMismatch(func(msg).into()),
            StringCacheMismatch(msg) => StringCacheMismatch(func(msg).into()),
            StructFieldNotFound(msg) => StructFieldNotFound(func(msg).into()),
        }
    }
}

pub fn map_err<E: Error>(error: E) -> PicachvError {
    PicachvError::ComputeError(format!("{error}").into())
}

#[macro_export]
macro_rules! picachv_err {
    ($variant:ident: $fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__private::must_use(
            $crate::PicachvError::$variant(format!($fmt, $($arg),*).into())
        )
    };
    ($variant:ident: $err:expr $(,)?) => {
        $crate::__private::must_use(
            $crate::PicachvError::$variant($err.into())
        )
    };
    (expr = $expr:expr, $variant:ident: $err:expr $(,)?) => {
        $crate::__private::must_use(
            $crate::PicachvError::$variant(
                format!("{}\n\nError originated in expression: '{:?}'", $err, $expr).into()
            )
        )
    };
    (expr = $expr:expr, $variant:ident: $fmt:literal, $($arg:tt)+) => {
        picachv_err!(expr = $expr, $variant: format!($fmt, $($arg)+))
    };
    (op = $op:expr, got = $arg:expr, expected = $expected:expr) => {
        $crate::picachv_err!(
            InvalidOperation: "{} operation not supported for dtype `{}` (expected: {})",
            $op, $arg, $expected
        )
    };
    (opq = $op:ident, got = $arg:expr, expected = $expected:expr) => {
        $crate::picachv_err!(
            op = concat!("`", stringify!($op), "`"), got = $arg, expected = $expected
        )
    };
    (un_impl = $op:ident) => {
        $crate::picachv_err!(
            InvalidOperation: "{} operation is not implemented.", concat!("`", stringify!($op), "`")
        )
    };
    (op = $op:expr, $arg:expr) => {
        $crate::picachv_err!(
            InvalidOperation: "{} operation not supported for dtype `{}`", $op, $arg
        )
    };
    (op = $op:expr, $lhs:expr, $rhs:expr) => {
        $crate::picachv_err!(
            InvalidOperation: "{} operation not supported for dtypes `{}` and `{}`", $op, $lhs, $rhs
        )
    };
    (oos = $($tt:tt)+) => {
        $crate::picachv_err!(ComputeError: "out-of-spec: {}", $($tt)+)
    };
    (nyi = $($tt:tt)+) => {
        $crate::picachv_err!(ComputeError: "not yet implemented: {}", format!($($tt)+) )
    };
    (opq = $op:ident, $arg:expr) => {
        $crate::picachv_err!(op = concat!("`", stringify!($op), "`"), $arg)
    };
    (opq = $op:ident, $lhs:expr, $rhs:expr) => {
        $crate::picachv_err!(op = stringify!($op), $lhs, $rhs)
    };
    (append) => {
        picachv_err!(SchemaMismatch: "cannot append series, data types don't match")
    };
    (extend) => {
        picachv_err!(SchemaMismatch: "cannot extend series, data types don't match")
    };
    (unpack) => {
        picachv_err!(SchemaMismatch: "cannot unpack series, data types don't match")
    };
    (not_in_enum,value=$value:expr,categories=$categories:expr) =>{
        picachv_err!(ComputeError: "value '{}' is not present in Enum: {:?}",$value,$categories)
    };
    (string_cache_mismatch) => {
        picachv_err!(StringCacheMismatch: r#"
cannot compare categoricals coming from different sources, consider setting a global StringCache.

Help: if you're using Python, this may look something like:

    with pl.StringCache():
        # Initialize Categoricals.
        df1 = pl.DataFrame({'a': ['1', '2']}, schema={'a': pl.Categorical})
        df2 = pl.DataFrame({'a': ['1', '3']}, schema={'a': pl.Categorical})
    # Your operations go here.
    pl.concat([df1, df2])

Alternatively, if the performance cost is acceptable, you could just set:

    import polars as pl
    pl.enable_string_cache()

on startup."#.trim_start())
    };
    (duplicate = $name:expr) => {
        picachv_err!(Duplicate: "column with name '{}' has more than one occurrences", $name)
    };
    (oob = $idx:expr, $len:expr) => {
        picachv_err!(OutOfBounds: "index {} is out of bounds for sequence of length {}", $idx, $len)
    };
    (agg_len = $agg_len:expr, $groups_len:expr) => {
        picachv_err!(
            ComputeError:
            "returned aggregation is of different length: {} than the groups length: {}",
            $agg_len, $groups_len
        )
    };
    (parse_fmt_idk = $dtype:expr) => {
        picachv_err!(
            ComputeError: "could not find an appropriate format to parse {}s, please define a format",
            $dtype,
        )
    };
}

#[macro_export]
macro_rules! picachv_bail {
    ($($tt:tt)+) => {
        return Err($crate::picachv_err!($($tt)+))
    };
}

#[macro_export]
macro_rules! picachv_ensure {
    ($cond:expr, $($tt:tt)+) => {
        if !$cond {
            picachv_bail!($($tt)+);
        }
    };
}

// Not public, referenced by macros only.
#[doc(hidden)]
pub mod __private {
    #[doc(hidden)]
    #[inline(always)]
    #[cold]
    #[must_use]
    pub fn must_use(error: crate::PicachvError) -> crate::PicachvError {
        error
    }
}
