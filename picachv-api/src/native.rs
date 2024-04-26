use picachv_core::dataframe::PolicyGuardedDataFrame;
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{ExprArgument, PlanArgument, TransformInfo};
use picachv_monitor::{PicachvMonitor, MONITOR_INSTANCE};
use uuid::Uuid;

macro_rules! impl_ctx_api {
    ($fn_name:ident, $ctx_fn:ident, $ctx_id:ident: Uuid, $($arg_name:ident: $arg_type:ty),* => $ret:ty) => {
        pub fn $fn_name($ctx_id: Uuid, $($arg_name: $arg_type),*) -> PicachvResult<$ret> {
            log::debug!("{} called for ctx_id: {}", stringify!($fn_name), $ctx_id);

            let instance = MONITOR_INSTANCE
                .get()
                .ok_or(PicachvError::InvalidOperation(
                    "Monitor not initialized".into(),
                ))?;

            let mut ctx = instance.get_ctx_mut()?;
            let ctx = ctx.get_mut(&$ctx_id).ok_or(PicachvError::InvalidOperation(
                "The context does not exist.".into(),
            ))?;

            ctx.$ctx_fn($($arg_name),*)
        }
    };
}

pub fn init_monitor() -> PicachvResult<()> {
    MONITOR_INSTANCE
        .set(PicachvMonitor::new().into())
        .map_err(|_| PicachvError::Already("Monitor initialized".into()))
}

pub fn open_new() -> PicachvResult<Uuid> {
    let instance = MONITOR_INSTANCE
        .get()
        .ok_or(PicachvError::InvalidOperation(
            "Monitor not initialized".into(),
        ))?;

    instance.open_new()
}

impl_ctx_api!(build_plan, plan_from_args, ctx_id: Uuid, plan_arg: PlanArgument => Uuid);
impl_ctx_api!(build_expr, expr_from_args, ctx_id: Uuid, expr_arg: ExprArgument => Uuid);
impl_ctx_api!(register_policy_dataframe, register_policy_dataframe, ctx_id: Uuid, df: PolicyGuardedDataFrame => Uuid);
impl_ctx_api!(execute_prologue, execute_prologue, ctx_id: Uuid, plan_uuid: Uuid, df_uuid: Uuid => Uuid);
impl_ctx_api!(execute_epilogue, execute_epilogue, ctx_id: Uuid, transform: TransformInfo => Uuid);
impl_ctx_api!(finalize, finalize, ctx_id: Uuid, df_uuid: Uuid => ());
