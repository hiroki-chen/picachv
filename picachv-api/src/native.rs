use picachv_core::dataframe::PolicyGuardedDataFrame;
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{ExprArgument, PlanArgument};
use picachv_monitor::MONITOR_INSTANCE;
use uuid::Uuid;

macro_rules! impl_ctx_api {
    ($fn_name:ident, $ctx_fn:ident, $ctx_id:ident: Uuid, $($arg_name:ident: $arg_type:ty),* => $ret:ty) => {
        pub fn $fn_name($ctx_id: Uuid, $($arg_name: $arg_type),*) -> PicachvResult<$ret> {
            tracing::debug!("{} called for ctx_id: {}", stringify!($fn_name), $ctx_id);

            let mut ctx = MONITOR_INSTANCE.get_ctx_mut()?;
            let ctx = ctx.get_mut(&$ctx_id).ok_or(PicachvError::InvalidOperation(
                "The context does not exist.".into(),
            ))?;

            ctx.$ctx_fn($($arg_name),*)
        }
    };
}

#[deprecated = "no need to call this method"]
pub fn init_monitor() -> PicachvResult<()> {
    Ok(())
}

pub fn open_new() -> PicachvResult<Uuid> {
    MONITOR_INSTANCE.open_new()
}

impl_ctx_api!(build_expr, expr_from_args, ctx_id: Uuid, expr_arg: ExprArgument => Uuid);
impl_ctx_api!(register_policy_dataframe, register_policy_dataframe, ctx_id: Uuid, df: PolicyGuardedDataFrame => Uuid);
impl_ctx_api!(register_policy_dataframe_json, register_policy_dataframe_json, ctx_id: Uuid, path: &str => Uuid);
impl_ctx_api!(register_policy_dataframe_bin, register_policy_dataframe_bin, ctx_id: Uuid, path: &str => Uuid);
impl_ctx_api!(register_policy_dataframe_parquet, register_policy_dataframe_parquet, ctx_id: Uuid, path: &str, projection: &[usize], predicate: Option<&[bool]> => Uuid);
impl_ctx_api!(execute_epilogue, execute_epilogue,
    ctx_id: Uuid, df_uuid: Uuid, plan_arg: Option<PlanArgument> => Uuid);
impl_ctx_api!(finalize, finalize, ctx_id: Uuid, df_uuid: Uuid => ());
impl_ctx_api!(reify_expression, reify_expression, ctx_id: Uuid, expr_uuid: Uuid, val: &[u8] => ());
impl_ctx_api!(enable_tracing, enable_tracing, ctx_id: Uuid, enable: bool => ());
impl_ctx_api!(enable_profiling, enable_profiling, ctx_id: Uuid, enable: bool => ());
