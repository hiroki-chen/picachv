use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use picachv_core::{
    dataframe::PolicyGuardedDataFrame, expr::Expr, get_new_uuid, plan::Plan, rwlock_unlock, Arenas,
};
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{
    transform_info::information::Information, ExprArgument, PlanArgument, TransformInfo,
};
use prost::Message;
use uuid::Uuid;

/// An activate context for the data analysis.
pub struct Context {
    /// The context ID.
    id: Uuid,
    /// A place for storing objects.
    arena: Arenas,
    /// The current plan root.
    root: Arc<RwLock<Uuid>>,
}

impl Context {
    pub fn new(id: Uuid) -> Self {
        Context {
            id,
            arena: Arenas::new(),
            root: Arc::new(RwLock::new(Uuid::default())),
        }
    }

    pub fn register_policy_dataframe(&self, df: PolicyGuardedDataFrame) -> PicachvResult<Uuid> {
        let mut df_arena = rwlock_unlock!(self.arena.df_arena, write);
        df_arena.insert(df)
    }

    pub fn plan_from_args(&mut self, plan_arg: PlanArgument) -> PicachvResult<Uuid> {
        let plan_arg = plan_arg.argument.ok_or(PicachvError::InvalidOperation(
            "The argument is empty.".into(),
        ))?;

        let plan = Plan::from_args(&self.arena, plan_arg)?;
        let mut lp_arena = rwlock_unlock!(self.arena.lp_arena, write);
        let root = lp_arena.insert(plan)?;
        *rwlock_unlock!(self.root, write) = root;

        Ok(root)
    }

    pub fn expr_from_args(&self, expr_arg: ExprArgument) -> PicachvResult<Uuid> {
        let expr_arg = expr_arg.argument.ok_or(PicachvError::InvalidOperation(
            "The argument is empty.".into(),
        ))?;

        let expr = Expr::from_args(&self.arena, expr_arg)?;
        let mut expr_arena = rwlock_unlock!(self.arena.expr_arena, write);
        let uuid = expr_arena.insert(expr)?;

        Ok(uuid)
    }

    pub fn execute_prologue(&self, plan_uuid: Uuid, df_uuid: Uuid) -> PicachvResult<Uuid> {
        let lp_arena = rwlock_unlock!(self.arena.lp_arena, read);
        let plan = lp_arena.get(&plan_uuid)?;

        plan.execute_prologue(&self.arena, df_uuid)
    }

    /// This function will parse the `TransformInfo` and apply the corresponding transformation on
    /// the invovled dataframes so that we can keep sync with the original dataframe since policies
    /// are de-coupled with their data. After succesful application it returns the UUID of the new
    /// dataframe allocated in the arena.
    pub fn execute_epilogue(&self, transform: TransformInfo) -> PicachvResult<Uuid> {
        let mut uuid = Uuid::default();

        for ti in transform.trans_info.iter() {
            match ti.information.as_ref() {
                Some(ti) => match ti {
                    Information::Dummy(dummy) => {
                        let df_uuid = Uuid::from_slice_le(&dummy.df_uuid)
                            .map_err(|_| PicachvError::InvalidOperation("Invalid UUID.".into()))?;
                        // Just a sanity check to make sure the UUID is valid.
                        rwlock_unlock!(self.arena.df_arena, read).get(&df_uuid)?;
                        // We just re-use the UUID.
                        uuid = df_uuid;
                    },

                    Information::Filter(pred) => {
                        let df_uuid = Uuid::from_slice_le(&pred.df_uuid)
                            .map_err(|_| PicachvError::InvalidOperation("Invalid UUID.".into()))?;
                        let mut df_arena = rwlock_unlock!(self.arena.df_arena, write);
                        let df = df_arena.get_mut(&df_uuid)?;

                        // We then apply the transformation.
                        //
                        // We first check if we are holding a strong reference to the dataframe, if so
                        // we can directly apply the transformation on the dataframe, otherwise we need
                        // to clone the dataframe and apply the transformation on the cloned dataframe.
                        // By doing so we can save the memory usage.
                        let new_uuid = match Arc::get_mut(df) {
                            Some(df) => {
                                df.filter(&pred.filter)?;
                                // We just re-use the UUID.
                                df_uuid
                            },
                            None => {
                                let mut df = (**df).clone();
                                df.filter(&pred.filter)?;
                                // We insert the new dataframe and this methods returns a new UUID.
                                df_arena.insert(df)?
                            },
                        };

                        uuid = new_uuid;
                    },
                    _ => todo!(),
                },
                None => {
                    return Err(PicachvError::InvalidOperation(
                        "The transformation information is empty.".into(),
                    ))
                },
            }
        }

        Ok(uuid)
    }

    pub fn finalize(&self, df_uuid: Uuid) -> PicachvResult<()> {
        let root = rwlock_unlock!(self.root, read);
        let lp_arena = rwlock_unlock!(self.arena.lp_arena, read);
        let df_arena = rwlock_unlock!(self.arena.df_arena, read);

        let plan = lp_arena.get(&root)?;
        log::debug!("Finalizing the plan {plan:?}");

        let df = df_arena.get(&df_uuid)?;
        df.finalize()
    }

    pub fn id(&self) -> Uuid {
        self.id
    }
}

/// The definition of our policy monitor.
pub struct PicachvMonitor {
    /// The context map.
    pub(crate) ctx: RwLock<HashMap<Uuid, Context>>,
}

impl PicachvMonitor {
    pub fn new() -> Self {
        PicachvMonitor {
            ctx: RwLock::new(HashMap::new()),
        }
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
        let ctx = Context::new(get_new_uuid());

        self.ctx
            .write()
            .map_err(|e| PicachvError::ComputeError(e.to_string().into()))?
            .insert(uuid, ctx);

        Ok(uuid)
    }

    pub fn build_plan(&self, ctx_id: Uuid, plan_arg: &[u8]) -> PicachvResult<Uuid> {
        log::debug!("build_plan");

        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        let plan_arg = PlanArgument::decode(plan_arg)
            .map_err(|e| PicachvError::InvalidOperation(e.to_string().into()))?;

        ctx.plan_from_args(plan_arg)
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

    pub fn execute_prologue(
        &self,
        ctx_id: Uuid,
        plan_uuid: Uuid,
        df_uuid: Uuid,
    ) -> PicachvResult<Uuid> {
        log::debug!("execute_prologue");

        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.execute_prologue(plan_uuid, df_uuid)
    }

    pub fn execute_epilogue(
        &self,
        ctx_id: Uuid,
        side_effect: TransformInfo,
    ) -> PicachvResult<Uuid> {
        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.execute_epilogue(side_effect)
    }

    pub fn finalize(&self, ctx_id: Uuid, df_uuid: Uuid) -> PicachvResult<()> {
        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.finalize(df_uuid)
    }
}

pub static MONITOR_INSTANCE: OnceLock<PicachvMonitor> = OnceLock::new();
