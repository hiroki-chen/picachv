use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use picachv_core::dataframe::{apply_transform, PolicyGuardedDataFrame};
use picachv_core::expr::Expr;
use picachv_core::plan::Plan;
use picachv_core::udf::Udf;
use picachv_core::{get_new_uuid, record_batch_from_bytes, rwlock_unlock, Arenas};
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{ExprArgument, PlanArgument, TransformInfo};
use prost::Message;
use uuid::Uuid;

/// An activate context for the data analysis.
#[readonly::make]
pub struct Context {
    /// The context ID.
    id: Uuid,
    /// A place for storing objects.
    arena: Arenas,
    /// The current plan root.
    root: Arc<RwLock<Uuid>>,
    /// UDFs.
    #[readonly]
    udfs: HashMap<String, Udf>,
}

impl Context {
    pub fn new(id: Uuid, udfs: HashMap<String, Udf>) -> Self {
        Context {
            id,
            arena: Arenas::new(),
            root: Arc::new(RwLock::new(Uuid::default())),
            udfs,
        }
    }

    pub fn register_policy_dataframe(&self, df: PolicyGuardedDataFrame) -> PicachvResult<Uuid> {
        let mut df_arena = rwlock_unlock!(self.arena.df_arena, write);
        df_arena.insert(df)
    }

    pub fn expr_from_args(&self, expr_arg: ExprArgument) -> PicachvResult<Uuid> {
        let expr_arg = expr_arg.argument.ok_or(PicachvError::InvalidOperation(
            "The argument is empty.".into(),
        ))?;

        let expr = Expr::from_args(&self.arena, expr_arg)?;
        log::debug!("expr_from_args: expr = {expr:?}");
        let mut expr_arena = rwlock_unlock!(self.arena.expr_arena, write);
        let uuid = expr_arena.insert(expr)?;
        log::debug!("expr_from_args: uuid = {uuid}");

        Ok(uuid)
    }

    // pub fn execute_prologue(&self, plan_uuid: Uuid, df_uuid: Uuid) -> PicachvResult<Uuid> {
    //     let lp_arena = rwlock_unlock!(self.arena.lp_arena, read);
    //     let plan = lp_arena.get(&plan_uuid)?;

    //     plan.execute_prologue(&self.arena, df_uuid, &self.udfs)
    // }

    /// This function will parse the `TransformInfo` and apply the corresponding transformation on
    /// the invovled dataframes so that we can keep sync with the original dataframe since policies
    /// are de-coupled with their data. After succesful application it returns the UUID of the new
    /// dataframe allocated in the arena.
    pub fn execute_epilogue(
        &self,
        mut df_uuid: Uuid,
        plan_arg: Option<PlanArgument>,
        transform: TransformInfo, // todo: possibly we can wrap it into `plan_arg`.
    ) -> PicachvResult<Uuid> {
        if let Some(plan_arg) = plan_arg {
            let arg = plan_arg.argument.ok_or(PicachvError::InvalidOperation(
                "The argument is empty.".into(),
            ))?;
            let plan = Plan::from_args(&self.arena, arg)?;
            df_uuid = plan.check_executor(&self.arena, df_uuid, &self.udfs)?;
        }

        if transform.trans_info.is_empty() {
            return Ok(df_uuid);
        }

        apply_transform(&self.arena.df_arena, transform)
    }

    pub fn finalize(&self, df_uuid: Uuid) -> PicachvResult<()> {
        let df_arena = rwlock_unlock!(self.arena.df_arena, read);

        let df = df_arena.get(&df_uuid)?;
        df.finalize()
    }

    /// Reify an abstract value of the expression with the given values encoded in the bytes.
    ///
    /// The input values are just a serialized Arrow IPC data represented as record batches.
    pub fn reify_expression(&self, expr_uuid: Uuid, value: &[u8]) -> PicachvResult<()> {
        log::debug!("reify_expression: expression uuid = {expr_uuid} ");

        let mut expr_arena = rwlock_unlock!(self.arena.expr_arena, write);
        let expr = expr_arena.get_mut(&expr_uuid)?;
        let expr = match Arc::get_mut(expr) {
            Some(expr) => expr,
            None => {
                return Err(PicachvError::InvalidOperation(
                    "The expression is immutable.".into(),
                ))
            },
        };

        if !expr.needs_reify() {
            return Err(PicachvError::InvalidOperation(
                "The expression does not need reify.".into(),
            ));
        }

        // Convert values into the Arrow record batch.
        let rb = record_batch_from_bytes(value)?;

        expr.reify(rb)?;
        Ok(())
    }

    pub fn id(&self) -> Uuid {
        self.id
    }
}

/// The definition of our policy monitor.
pub struct PicachvMonitor {
    /// The context map.
    pub(crate) ctx: Arc<RwLock<HashMap<Uuid, Context>>>,
    pub(crate) udfs: Arc<RwLock<HashMap<String, Udf>>>,
}

impl PicachvMonitor {
    pub fn new() -> Self {
        PicachvMonitor {
            ctx: Arc::new(RwLock::new(HashMap::new())),
            udfs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register_new_udf(&self, udf: Udf) -> PicachvResult<()> {
        let mut udfs = rwlock_unlock!(self.udfs, write);
        udfs.insert(udf.name().to_string(), udf);
        Ok(())
    }

    pub fn get_ctx(&self) -> PicachvResult<RwLockReadGuard<HashMap<Uuid, Context>>> {
        Ok(rwlock_unlock!(self.ctx, read))
    }

    pub fn get_ctx_mut(&self) -> PicachvResult<RwLockWriteGuard<HashMap<Uuid, Context>>> {
        Ok(rwlock_unlock!(self.ctx, write))
    }

    /// Opens a new context.
    pub fn open_new(&self) -> PicachvResult<Uuid> {
        let uuid = get_new_uuid();
        let udfs = rwlock_unlock!(self.udfs, read).clone();
        let ctx = Context::new(get_new_uuid(), udfs);

        self.ctx
            .write()
            .map_err(|e| PicachvError::ComputeError(e.to_string().into()))?
            .insert(uuid, ctx);

        Ok(uuid)
    }

    pub fn build_expr(&self, ctx_id: Uuid, expr_arg: &[u8]) -> PicachvResult<Uuid> {
        log::debug!("build_expr");

        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;
        let expr_arg = ExprArgument::decode(expr_arg)
            .map_err(|e| PicachvError::InvalidOperation(e.to_string().into()))?;

        ctx.expr_from_args(expr_arg)
    }

    /// This function must be invoked after an executor is about to return the data to the
    /// (possibly) higher level executor. This function is used to fulfill the following:
    ///
    /// - Check if the operation is allowed by the policy.
    /// - Apply the transformation on the involved dataframes.
    ///
    /// Why do we check afterwards rather than before? The reason is that we need access to
    /// the reified values which are only known after the execution of the plan.
    pub fn execute_epilogue(
        &self,
        ctx_id: Uuid,
        df_uuid: Uuid,
        plan_arg: Option<PlanArgument>,
        side_effect: TransformInfo,
    ) -> PicachvResult<Uuid> {
        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.execute_epilogue(df_uuid, plan_arg, side_effect)
    }

    pub fn finalize(&self, ctx_id: Uuid, df_uuid: Uuid) -> PicachvResult<()> {
        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.finalize(df_uuid)
    }
}

pub static MONITOR_INSTANCE: OnceLock<Arc<PicachvMonitor>> = OnceLock::new();
