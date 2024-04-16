/// This function should be called whenever the caller is about to add a new node to the plan tree,
/// which should be called by the logical planner.
/// If this successfully returns 0 then we are fine, otherwise we need to abort since this plan already
/// violates the security policy.
/// 
/// - `ctx`: The opaque handle to the monitor context.
/// - `build_args`: A pointer to the serialized argument struct.
/// - `build_args_size`: The length of `build_arg`.
#[no_mangle]
pub extern "C" fn build_logical_node(
  ctx: usize,
  build_args: *const u8,
  build_args_size: usize,
) -> i32 {
  0
}
