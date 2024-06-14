use std::sync::{OnceLock, RwLock};

use picachv_core::dataframe::PolicyGuardedDataFrame;
use picachv_core::io::JsonIO;
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{ExprArgument, PlanArgument};
use picachv_monitor::{PicachvMonitor, MONITOR_INSTANCE};
use prost::Message;
use uuid::Uuid;

/// The last error message.
static LAST_ERROR: OnceLock<RwLock<String>> = OnceLock::new();

macro_rules! try_execute {
    ($expr:expr) => {{
        match $expr {
            Ok(val) => val,
            Err(err) => {
                let mut s = LAST_ERROR
                    .get_or_init(|| RwLock::new("".into()))
                    .write()
                    .unwrap();
                *s = format!("{}", err);
                return err.into();
            },
        }
    }};

    ($expr:expr, $err:expr) => {{
        match $expr {
            Ok(val) => val,
            Err(err) => {
                let mut s = LAST_ERROR
                    .get_or_init(|| RwLock::new("".into()))
                    .write()
                    .unwrap();
                *s = format!("{}", err);
                return $err;
            },
        }
    }};
}

#[repr(i32)]
pub enum ErrorCode {
    Success = 0,
    InvalidOperation = 1,
    SerializeError = 2,
    NoEntry = 3,
    PrivacyBreach = 4,
    Already = 5,
}

impl From<PicachvError> for ErrorCode {
    fn from(err: PicachvError) -> Self {
        match err {
            PicachvError::InvalidOperation(_) => ErrorCode::InvalidOperation,
            PicachvError::PrivacyError(_) => ErrorCode::PrivacyBreach,
            PicachvError::Already(_) => ErrorCode::Already,
            _ => ErrorCode::InvalidOperation,
        }
    }
}

/// A convenient wrapper for recovering a UUID from a pointer.
fn recover_uuid(uuid_ptr: *const u8, len: usize) -> PicachvResult<Uuid> {
    let uuid_bytes = unsafe { std::slice::from_raw_parts(uuid_ptr, len) };
    match Uuid::from_slice_le(uuid_bytes) {
        Ok(uuid) => Ok(uuid),
        Err(_) => Err(PicachvError::InvalidOperation(
            "Failed to recover the UUID.".into(),
        )),
    }
}

#[no_mangle]
pub extern "C" fn last_error(output: *mut u8, output_len: *mut usize) {
    let s = LAST_ERROR
        .get_or_init(|| RwLock::new("".into()))
        .read()
        .unwrap();

    unsafe {
        let len = s.len();
        *output_len = len;
        std::ptr::copy_nonoverlapping(s.as_ptr(), output, len);
    }
}

/// Registers a policy-guarded dataframe into the context.
#[no_mangle]
pub extern "C" fn register_policy_dataframe(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    df: *const u8,
    df_len: usize,
    df_uuid: *mut u8,
    df_uuid_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));

    let df = unsafe { std::slice::from_raw_parts(df, df_len) };
    // Recover from bytes and register the dataframe.
    let df = try_execute!(PolicyGuardedDataFrame::from_json_bytes(df));

    let ctx = match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.get_ctx() {
            Ok(ctx) => ctx,
            Err(_) => return ErrorCode::InvalidOperation,
        },
        None => return ErrorCode::NoEntry,
    };

    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let res = try_execute!(ctx.register_policy_dataframe(df));

    unsafe {
        std::ptr::copy_nonoverlapping(res.to_bytes_le().as_ptr(), df_uuid, df_uuid_len);
    }

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn init_monitor() -> ErrorCode {
    match MONITOR_INSTANCE.set(PicachvMonitor::new().into()) {
        Ok(_) => ErrorCode::Success,
        // Already.
        Err(_) => ErrorCode::Already,
    }
}

#[no_mangle]
pub extern "C" fn open_new(uuid_ptr: *mut u8, len: usize) -> ErrorCode {
    if len < 16 {
        return ErrorCode::InvalidOperation;
    }

    match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.open_new() {
            Ok(uuid) => {
                let uuid_bytes = uuid.to_bytes_le();
                println!("copying the uuid to the pointer");
                unsafe { std::ptr::copy(uuid_bytes.as_ptr(), uuid_ptr, uuid_bytes.len()) }
                println!("copying the uuid to the pointer finished");
                tracing::debug!("returning {uuid:?}");
                ErrorCode::Success
            },
            Err(_) => ErrorCode::InvalidOperation,
        },
        None => ErrorCode::InvalidOperation,
    }
}

/// Constructs the expression out of the argument which is a serialized protobuf
/// byte array that can be deserialized into an `ExprArgument`.
#[no_mangle]
pub extern "C" fn expr_from_args(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    expr_arg: *const u8,
    expr_arg_len: usize,
    expr_uuid: *mut u8,
    expr_uuid_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));

    let ctx = match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.get_ctx() {
            Ok(ctx) => ctx,
            Err(e) => return e.into(),
        },
        None => return ErrorCode::NoEntry,
    };

    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let expr_arg = unsafe { std::slice::from_raw_parts(expr_arg, expr_arg_len) };
    let expr_arg = try_execute!(ExprArgument::decode(expr_arg), ErrorCode::SerializeError);

    if expr_uuid_len != 16 {
        return ErrorCode::InvalidOperation;
    }

    let uuid = try_execute!(ctx.expr_from_args(expr_arg));

    unsafe {
        std::ptr::copy_nonoverlapping(uuid.to_bytes_le().as_ptr(), expr_uuid, expr_uuid_len);
    }

    ErrorCode::Success
}

/// Reifies an expression if the value is provided.
#[no_mangle]
pub extern "C" fn reify_expression(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    expr_uuid: *const u8,
    expr_uuid_len: usize,
    value: *const u8,
    value_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let expr_id = try_execute!(recover_uuid(expr_uuid, expr_uuid_len));

    let ctx = match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.get_ctx() {
            Ok(ctx) => ctx,
            Err(_) => return ErrorCode::InvalidOperation,
        },
        None => return ErrorCode::NoEntry,
    };

    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let value = unsafe { std::slice::from_raw_parts(value, value_len) };

    try_execute!(ctx.reify_expression(expr_id, value));

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn create_slice(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    df_uuid: *const u8,
    df_len: usize,
    start: usize,
    end: usize,
    slice_df_uuid: *mut u8,
    slice_df_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let df_id = try_execute!(recover_uuid(df_uuid, df_len));

    let ctx = match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.get_ctx() {
            Ok(ctx) => ctx,
            Err(_) => return ErrorCode::InvalidOperation,
        },
        None => return ErrorCode::NoEntry,
    };

    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let out = try_execute!(ctx.create_slice(df_id, start..end));
    unsafe {
        std::ptr::copy_nonoverlapping(out.to_bytes_le().as_ptr(), slice_df_uuid, slice_df_len);
    }

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn finalize(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    df_uuid: *const u8,
    df_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let df_id = try_execute!(recover_uuid(df_uuid, df_len));

    let ctx = match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.get_ctx() {
            Ok(ctx) => ctx,
            Err(_) => return ErrorCode::InvalidOperation,
        },
        None => return ErrorCode::NoEntry,
    };

    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    try_execute!(ctx.finalize(df_id));

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn early_projection(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    df_uuid: *const u8,
    df_len: usize,
    project_list: *const usize,
    project_list_len: usize,
    proj_df_uuid: *mut u8,
    proj_df_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let df_id = try_execute!(recover_uuid(df_uuid, df_len));

    let ctx = match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.get_ctx() {
            Ok(ctx) => ctx,
            Err(_) => return ErrorCode::InvalidOperation,
        },
        None => return ErrorCode::NoEntry,
    };

    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };
    let project_list = unsafe { std::slice::from_raw_parts(project_list, project_list_len) };

    let out = try_execute!(ctx.early_projection(df_id, project_list));
    unsafe {
        std::ptr::copy_nonoverlapping(out.to_bytes_le().as_ptr(), proj_df_uuid, proj_df_len);
    }

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn execute_epilogue(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    plan_arg: *const u8,
    plan_arg_len: usize,
    df_uuid: *const u8,
    df_len: usize,
    output: *mut u8,
    output_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let df_id = try_execute!(recover_uuid(df_uuid, df_len));

    let plan_arg = unsafe {
        if plan_arg.is_null() {
            None
        } else {
            let bytes = std::slice::from_raw_parts(plan_arg, plan_arg_len);
            Some(try_execute!(
                PlanArgument::decode(bytes),
                ErrorCode::SerializeError
            ))
        }
    };

    let ctx = match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.get_ctx() {
            Ok(ctx) => ctx,
            Err(_) => return ErrorCode::InvalidOperation,
        },
        None => return ErrorCode::NoEntry,
    };

    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    println!("execute_epilogue: df_id = {df_id:?}, plan_arg = {plan_arg:?}");
    let out = try_execute!(ctx.execute_epilogue(df_id, plan_arg));
    println!("out = {out:?}");

    unsafe {
        std::ptr::copy_nonoverlapping(out.to_bytes_le().as_ptr(), output, output_len);
    }

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn debug_print_df(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    df_uuid: *const u8,
    df_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let df_id = try_execute!(recover_uuid(df_uuid, df_len));

    let ctx = match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.get_ctx() {
            Ok(ctx) => ctx,
            Err(_) => return ErrorCode::InvalidOperation,
        },
        None => return ErrorCode::NoEntry,
    };

    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let df = try_execute!(ctx.get_df(df_id));
    println!("{df}");

    ErrorCode::Success
}

#[no_mangle]
pub extern "C" fn register_policy_dataframe_json(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    path: *const u8,
    path_len: usize,
    df_uuid: *mut u8,
    df_uuid_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));

    let path = unsafe { std::slice::from_raw_parts(path, path_len) };
    let path = std::str::from_utf8(path).unwrap();

    let df = try_execute!(PolicyGuardedDataFrame::from_json(path));

    let ctx = match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.get_ctx() {
            Ok(ctx) => ctx,
            Err(_) => return ErrorCode::InvalidOperation,
        },
        None => return ErrorCode::NoEntry,
    };

    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let res = try_execute!(ctx.register_policy_dataframe(df));

    unsafe {
        std::ptr::copy_nonoverlapping(res.to_bytes_le().as_ptr(), df_uuid, df_uuid_len);
    }

    ErrorCode::Success
}
