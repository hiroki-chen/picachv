use picachv_core::callback::{Callable, Caller, BUF_SIZE};
use picachv_error::{PicachvError, PicachvResult};
use picachv_monitor::{PicachvMonitor, MONITOR_INSTANCE};
use uuid::Uuid;

/// The callback function written in C so it remains opaque to the Rust code.
type Callback = unsafe extern "C" fn(*mut u8, *mut usize) -> i32;

struct CallbackWrapper {
    cb: Callback,
}

impl Callable for CallbackWrapper {
    fn call(&self) -> PicachvResult<Vec<u8>> {
        let mut transformation_info = vec![0u8; BUF_SIZE];
        let mut len = transformation_info.len();

        let result = unsafe { (self.cb)(transformation_info.as_mut_ptr(), &mut len) };

        if result != 0 {
            return Err(PicachvError::InvalidOperation(
                "The callback function failed.".into(),
            ));
        }

        Ok(transformation_info[..len].to_vec())
    }

    fn box_clone(&self) -> Box<dyn Callable> {
        Box::new(Self { cb: self.cb })
    }
}

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
    cb: Callback,
) -> i32 {
    if ctx_uuid_len != 16 || uuid_len != 16 {
        return 1;
    }

    let ctx_uuid = match recover_uuid(ctx_uuid, ctx_uuid_len) {
        Ok(uuid) => uuid,
        Err(_) => return 1,
    };

    println!("getting {ctx_uuid:?}");
    let caller = Caller::new(CallbackWrapper { cb });

    match MONITOR_INSTANCE.get() {
        Some(monitor) => {
            let arg = unsafe { std::slice::from_raw_parts(build_args, build_args_size) };

            match monitor.build_plan(ctx_uuid, arg, caller) {
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
    match MONITOR_INSTANCE.set(PicachvMonitor::new()) {
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
                println!("returning {uuid:?}");
                0
            },
            Err(_) => 1,
        },
        None => 1,
    }
}

#[no_mangle]
pub extern "C" fn execute(uuid_ptr: *mut u8, len: usize) -> i32 {
    if len < 16 {
        return 1;
    }

    let uuid = match recover_uuid(uuid_ptr, len) {
        Ok(uuid) => uuid,
        Err(_) => return 1,
    };

    match MONITOR_INSTANCE.get() {
        Some(monitor) => match monitor.execute(uuid) {
            Ok(_) => 0,
            Err(_) => 1,
        },
        None => 1,
    }
}
