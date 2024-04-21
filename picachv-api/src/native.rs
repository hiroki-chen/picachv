use picachv_core::callback::Caller;
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::PlanArgument;
use picachv_monitor::{PicachvMonitor, MONITOR_INSTANCE};
use uuid::Uuid;

pub fn init_monitor() -> PicachvResult<()> {
    MONITOR_INSTANCE
        .set(PicachvMonitor::new())
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

pub fn build_plan(ctx_id: Uuid, plan_arg: PlanArgument, cb: Caller) -> PicachvResult<Uuid> {
    let instance = MONITOR_INSTANCE
        .get()
        .ok_or(PicachvError::InvalidOperation(
            "Monitor not initialized".into(),
        ))?;

    let mut ctx = instance.get_ctx_mut()?;
    let ctx = ctx.get_mut(&ctx_id).ok_or(PicachvError::InvalidOperation(
        "The context does not exist.".into(),
    ))?;

    ctx.plan_from_args(plan_arg, cb)
}
