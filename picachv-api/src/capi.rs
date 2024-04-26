use picachv_error::{PicachvError, PicachvResult};
use picachv_monitor::{PicachvMonitor, MONITOR_INSTANCE};
use uuid::Uuid;

fn recover_uuid(uuid_ptr: *const u8, len: usize) -> PicachvResult<Uuid> {
    let uuid_bytes = unsafe { std::slice::from_raw_parts(uuid_ptr, len) };
    match Uuid::from_slice_le(uuid_bytes) {
        Ok(uuid) => Ok(uuid),
        Err(_) => Err(PicachvError::InvalidOperation(
            "Failed to recover the UUID.".into(),
        )),
    }
}

/// This function should be called whenever the caller is about to add a new node to the plan tree,
/// which should be called by the logical planner.
/// If this successfully returns 0 then we are fine, otherwise we need to abort since this plan already
/// violates the security policy.
///
/// - `ctx`: The Uuid to the context.
/// - `build_args`: A pointer to the serialized argument struct.
/// - `build_args_size`: The length of `build_arg`.
#[no_mangle]
pub extern "C" fn build_plan(
    ctx_uuid: *const u8,
    ctx_uuid_len: usize,
    build_args: *const u8,
    build_args_size: usize,
    uuid_ptr: *mut u8,
    uuid_len: usize,
) -> i32 {
    if ctx_uuid_len != 16 || uuid_len != 16 {
        return 1;
    }

    let ctx_uuid = match recover_uuid(ctx_uuid, ctx_uuid_len) {
        Ok(uuid) => uuid,
        Err(_) => return 1,
    };

    log::debug!("getting {ctx_uuid:?}");
    match MONITOR_INSTANCE.get() {
        Some(monitor) => {
            let arg = unsafe { std::slice::from_raw_parts(build_args, build_args_size) };

            match monitor.build_plan(ctx_uuid, arg) {
                Ok(uuid) => {
                    unsafe {
                        std::ptr::copy_nonoverlapping(uuid.as_bytes().as_ptr(), uuid_ptr, 16);
                    }

                    0
                },
                Err(_) => 1,
            }
        },
        None => 1,
    }
}

#[no_mangle]
pub extern "C" fn init_monitor() -> i32 {
    match MONITOR_INSTANCE.set(PicachvMonitor::new().into()) {
        Ok(_) => 0,
        // Already.
        Err(_) => 1,
    }
}

#[no_mangle]
pub extern "C" fn open_new(uuid_ptr: *mut u8, len: usize) -> i32 {
    if len < 16 {
        return 1;
    }

    match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.open_new() {
            Ok(uuid) => {
                let uuid_bytes = uuid.to_bytes_le();
                unsafe {
                    std::ptr::copy_nonoverlapping(uuid_bytes.as_ptr(), uuid_ptr, uuid_bytes.len())
                }
                log::debug!("returning {uuid:?}");
                0
            },
            Err(_) => 1,
        },
        None => 1,
    }
}

#[no_mangle]
pub extern "C" fn execute_prologue(
    ctx_uuid_ptr: *const u8,
    ctx_uuid_ptr_len: usize,
    plan_uuid_ptr: *const u8,
    plan_uuid_ptr_len: usize,
    df_uuid_ptr: *const u8,
    df_uuid_ptr_len: usize,
) -> i32 {
    if ctx_uuid_ptr_len < 16 {
        return 1;
    }

    let (ctx_id, plan_id, df_id) = match (
        recover_uuid(ctx_uuid_ptr, ctx_uuid_ptr_len),
        recover_uuid(plan_uuid_ptr, plan_uuid_ptr_len),
        recover_uuid(df_uuid_ptr, df_uuid_ptr_len),
    ) {
        (Ok(ctx_id), Ok(plan_id), Ok(df_id)) => (ctx_id, plan_id, df_id),
        _ => return 1,
    };

    match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.execute_prologue(ctx_id, plan_id, df_id) {
            Ok(_) => 0,
            Err(_) => 1,
        },
        None => 1,
    }
}
