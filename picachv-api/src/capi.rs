use std::sync::LazyLock;

use picachv_core::dataframe::PolicyGuardedDataFrame;
use picachv_core::io::JsonIO;
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{ExprArgument, PlanArgument};
use picachv_monitor::MONITOR_INSTANCE;
use prost::Message;
use spin::RwLock;
use uuid::Uuid;

#[repr(C)]
#[derive(Debug)]
pub struct RegisterFromRgArgs {
    path: *const u8,
    path_len: usize,
    row_group: usize,
    df_uuid: *mut u8,
    df_uuid_len: usize,
    projection: *const usize,
    projection_len: usize,
    selection: *const bool,
    selection_len: usize,
}

/// The last error message.
static LAST_ERROR: LazyLock<RwLock<String>> = LazyLock::new(|| RwLock::new(String::new()));

macro_rules! try_execute {
    ($expr:expr) => {{
        match $expr {
            Ok(val) => val,
            Err(err) => {
                let mut s = LAST_ERROR.write();
                *s = format!("{}", err);
                return err.into();
            },
        }
    }};

    ($expr:expr, $err:expr) => {{
        match $expr {
            Ok(val) => val,
            Err(err) => {
                let mut s = LAST_ERROR.write();
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
unsafe fn recover_uuid(uuid_ptr: *const u8, len: usize) -> PicachvResult<Uuid> {
    if uuid_ptr.is_null() {
        return Ok(Default::default());
    }

    let uuid_bytes = std::slice::from_raw_parts(uuid_ptr, len);
    match Uuid::from_slice_le(uuid_bytes) {
        Ok(uuid) => Ok(uuid),
        Err(_) => Err(PicachvError::InvalidOperation(
            "Failed to recover the UUID.".into(),
        )),
    }
}

#[no_mangle]
pub unsafe extern "C" fn last_error(output: *mut u8, output_len: *mut usize) {
    let s = LAST_ERROR.read();

    let len = if s.len() < 65535 { s.len() } else { 65535 };
    *output_len = len;
    std::ptr::copy_nonoverlapping(s.as_ptr(), output, len);
}

/// Registers a policy-guarded dataframe into the context.
#[no_mangle]
pub unsafe extern "C" fn register_policy_dataframe(
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

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
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
pub unsafe extern "C" fn open_new(uuid_ptr: *mut u8, len: usize) -> ErrorCode {
    if len < 16 {
        return ErrorCode::InvalidOperation;
    }

    match MONITOR_INSTANCE.write().open_new() {
        Ok(uuid) => {
            let uuid_bytes = uuid.to_bytes_le();
            std::ptr::copy(uuid_bytes.as_ptr(), uuid_ptr, uuid_bytes.len());
            tracing::debug!("returning {uuid:?}");
            ErrorCode::Success
        },
        Err(_) => ErrorCode::InvalidOperation,
    }
}

/// Constructs the expression out of the argument which is a serialized protobuf
/// byte array that can be deserialized into an `ExprArgument`.
#[no_mangle]
pub unsafe extern "C" fn expr_from_args(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    expr_arg: *const u8,
    expr_arg_len: usize,
    expr_uuid: *mut u8,
    expr_uuid_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let expr_arg = std::slice::from_raw_parts(expr_arg, expr_arg_len);
    let expr_arg = try_execute!(ExprArgument::decode(expr_arg), ErrorCode::SerializeError);

    if expr_uuid_len != 16 {
        return ErrorCode::InvalidOperation;
    }

    let uuid = try_execute!(ctx.expr_from_args(expr_arg));

    std::ptr::copy_nonoverlapping(uuid.to_bytes_le().as_ptr(), expr_uuid, expr_uuid_len);

    ErrorCode::Success
}

/// Reifies an expression if the value is provided.
#[no_mangle]
pub unsafe extern "C" fn reify_expression(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    expr_uuid: *const u8,
    expr_uuid_len: usize,
    value: *const u8,
    value_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let expr_id = try_execute!(recover_uuid(expr_uuid, expr_uuid_len));

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let value = std::slice::from_raw_parts(value, value_len);

    try_execute!(ctx.reify_expression(expr_id, value));

    ErrorCode::Success
}

// FIXME: Should be a select vector!
#[no_mangle]
pub unsafe extern "C" fn create_slice(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    df_uuid: *const u8,
    df_len: usize,
    sel_vec: *const u32,
    sel_vec_len: usize,
    slice_df_uuid: *mut u8,
    slice_df_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let df_id = try_execute!(recover_uuid(df_uuid, df_len));

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let sel_vec = std::slice::from_raw_parts(sel_vec, sel_vec_len);

    let out = try_execute!(ctx.create_slice(df_id, sel_vec));

    std::ptr::copy_nonoverlapping(out.to_bytes_le().as_ptr(), slice_df_uuid, slice_df_len);

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn finalize(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    df_uuid: *const u8,
    df_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let df_id = try_execute!(recover_uuid(df_uuid, df_len));

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    try_execute!(ctx.finalize(df_id));

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn early_projection(
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

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };
    let project_list = std::slice::from_raw_parts(project_list, project_list_len);

    let out = try_execute!(ctx.early_projection(df_id, project_list));

    std::ptr::copy_nonoverlapping(out.to_bytes_le().as_ptr(), proj_df_uuid, proj_df_len);

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn execute_epilogue(
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

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let out = try_execute!(ctx.execute_epilogue(df_id, plan_arg));

    std::ptr::copy_nonoverlapping(out.to_bytes_le().as_ptr(), output, output_len);

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn debug_print_df(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    df_uuid: *const u8,
    df_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let df_id = try_execute!(recover_uuid(df_uuid, df_len));

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let df = try_execute!(ctx.get_df(df_id));
    println!("{df}(shape: {:?})", df.shape());

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn register_policy_dataframe_from_row_group(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    args: *const RegisterFromRgArgs,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let args = &*args;

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let path =
        String::from_utf8(std::slice::from_raw_parts(args.path, args.path_len).to_vec()).unwrap();
    let projection = std::slice::from_raw_parts(args.projection, args.projection_len);
    let selection = match args.selection.is_null() {
        true => None,
        false => Some(std::slice::from_raw_parts(
            args.selection,
            args.selection_len,
        )),
    };

    let uuid = try_execute!(ctx.register_policy_dataframe_from_row_group(
        path,
        projection,
        selection,
        args.row_group
    ));
    std::ptr::copy_nonoverlapping(uuid.to_bytes_le().as_ptr(), args.df_uuid, args.df_uuid_len);

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn enable_profiling(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    enable: bool,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    try_execute!(ctx.enable_profiling(enable));

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn enable_tracing(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    enable: bool,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
    let ctx = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    try_execute!(ctx.enable_tracing(enable));

    ErrorCode::Success
}

#[no_mangle]
pub unsafe extern "C" fn select_group(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    df_uuid: *const u8,
    df_len: usize,
    hash: *const u64,
    hash_len: usize,
    out_df_uuid: *mut u8,
    out_df_len: usize,
) -> ErrorCode {
    let ctx_id = try_execute!(recover_uuid(ctx_uuid, ctx_uuid_len));
    let df_id: Uuid = try_execute!(recover_uuid(df_uuid, df_len));

    let ctx = MONITOR_INSTANCE.read();
    let ctx = ctx.get_ctx();
    let ctx: &picachv_monitor::Context = match ctx.get(&ctx_id) {
        Some(ctx) => ctx,
        None => return ErrorCode::NoEntry,
    };

    let hash = std::slice::from_raw_parts(hash, hash_len);
    let out = try_execute!(ctx.select_group(df_id, hash));

    std::ptr::copy_nonoverlapping(out.to_bytes_le().as_ptr(), out_df_uuid, out_df_len);

    ErrorCode::Success
}
