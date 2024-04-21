use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use picachv_core::{
    callback::Caller,
    dataframe::{DataFrameRegistry, PolicyGuardedDataFrame},
    get_new_uuid,
    plan::Plan,
    rwlock_unlock, Arenas,
};
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{ExprArgument, PlanArgument};
use prost::Message;
use uuid::Uuid;

/// An activate context for the data analysis.
pub struct Context {
    /// The context ID.
    id: Uuid,
    /// The dataframes (w/o data) to be analyzed.
    df_registry: DataFrameRegistry,
    /// A place for storing objects.
    arena: Arenas,
    /// The current plan root.
    root: Uuid,
}

impl Context {
    pub fn new(id: Uuid) -> Self {
        Context {
            id,
            df_registry: DataFrameRegistry::new(),
            arena: Arenas::new(),
            root: Uuid::default(),
        }
    }

    pub fn register_policy_dataframe(&mut self, df: PolicyGuardedDataFrame) -> PicachvResult<Uuid> {
        let uuid = get_new_uuid();
        self.df_registry.insert(uuid, Arc::new(df));

        Ok(uuid)
    }

    pub fn plan_from_args(&mut self, plan_arg: PlanArgument, cb: Caller) -> PicachvResult<Uuid> {
        let mut lp_arena = rwlock_unlock!(self.arena.lp_arena, write);

        let plan_arg = plan_arg.argument.ok_or(PicachvError::InvalidOperation(
            "The argument is empty.".into(),
        ))?;

        println!("calling callback!");
        cb.call()?;
        println!("called callback!");

        let plan = Plan::from_args(&self.arena, plan_arg, cb)?;
        let root = lp_arena.insert(plan)?;
        self.root = root;

        Ok(root)
    }

    pub fn expr_from_args(&mut self, expr_arg: ExprArgument) -> PicachvResult<Uuid> {
        todo!()
    }

    pub fn execute(&mut self) -> PicachvResult<()> {
        let lp_arena = rwlock_unlock!(self.arena.lp_arena, read);
        let plan = lp_arena.get(&self.root)?;

        plan.execute()
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

    pub fn build_plan(&self, ctx_id: Uuid, plan_arg: &[u8], cb: Caller) -> PicachvResult<Uuid> {
        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        let plan_arg = PlanArgument::decode(plan_arg)
            .map_err(|e| PicachvError::InvalidOperation(e.to_string().into()))?;

        ctx.plan_from_args(plan_arg, cb)
    }

    pub fn build_expr(&self, ctx_id: Uuid, expr_arg: &[u8]) -> PicachvResult<Uuid> {
        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;
        let expr_arg = ExprArgument::decode(expr_arg)
            .map_err(|e| PicachvError::InvalidOperation(e.to_string().into()))?;

        ctx.expr_from_args(expr_arg)
    }

    pub fn execute(&self, ctx_id: Uuid) -> PicachvResult<()> {
        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.execute()
    }
}

pub static MONITOR_INSTANCE: OnceLock<PicachvMonitor> = OnceLock::new();
