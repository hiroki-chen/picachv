#![feature(lazy_cell)]

use std::collections::HashMap;
use std::fmt;
use std::fs::OpenOptions;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, LazyLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use picachv_core::dataframe::{apply_transform, PolicyGuardedDataFrame};
use picachv_core::expr::{ColumnIdent, Expr};
use picachv_core::io::{BinIo, JsonIO, ParquetIO};
use picachv_core::plan::{early_projection, Plan};
use picachv_core::profiler::PROFILER;
use picachv_core::udf::Udf;
use picachv_core::{get_new_uuid, record_batch_from_bytes, rwlock_unlock, Arenas};
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{plan_argument, ContextOptions, ExprArgument, PlanArgument};
use prost::Message;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
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
    /// Context options.
    pub(crate) options: ContextOptions,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context").field("id", &self.id).finish()
    }
}

impl Context {
    #[inline]
    pub fn enable_tracing(&mut self, enable: bool) -> PicachvResult<()> {
        self.options.enable_tracing = enable;

        Ok(())
    }

    #[inline]
    pub fn tracing_enabled(&self) -> bool {
        self.options.enable_tracing
    }

    #[inline]
    pub fn enable_profiling(&mut self, enable: bool) -> PicachvResult<()> {
        self.options.enable_profiling = enable;

        Ok(())
    }

    #[inline]
    pub fn profiling_enabled(&self) -> bool {
        self.options.enable_profiling
    }

    #[inline]
    pub fn new(id: Uuid, udfs: HashMap<String, Udf>) -> Self {
        Context {
            id,
            arena: Arenas::new(),
            root: Arc::new(RwLock::new(Uuid::default())),
            udfs,
            options: ContextOptions::default(),
        }
    }

    #[inline]
    #[tracing::instrument]
    pub fn register_policy_dataframe(&self, df: PolicyGuardedDataFrame) -> PicachvResult<Uuid> {
        let mut df_arena = rwlock_unlock!(self.arena.df_arena, write);
        df_arena.insert(df)
    }

    #[tracing::instrument]
    pub fn register_policy_dataframe_json<P: AsRef<Path> + fmt::Debug>(
        &self,
        path: P,
    ) -> PicachvResult<Uuid> {
        let df = PolicyGuardedDataFrame::from_json(path.as_ref())?;
        self.register_policy_dataframe(df)
    }

    #[tracing::instrument]
    pub fn register_policy_dataframe_bin<P: AsRef<Path> + fmt::Debug>(
        &self,
        path: P,
    ) -> PicachvResult<Uuid> {
        let df = PolicyGuardedDataFrame::from_bytes(path.as_ref())?;
        self.register_policy_dataframe(df)
    }

    #[tracing::instrument]
    pub fn register_policy_dataframe_parquet<P: AsRef<Path> + fmt::Debug>(
        &self,
        path: P,
        projection: &[usize],
        selection: Option<&[bool]>,
    ) -> PicachvResult<Uuid> {
        let df = if self.options.enable_profiling {
            PROFILER.profile(
                || PolicyGuardedDataFrame::from_parquet(path.as_ref(), projection, selection),
                "read_parquet".into(),
            )
        } else {
            PolicyGuardedDataFrame::from_parquet(path.as_ref(), projection, selection)
        }?;
        self.register_policy_dataframe(df)
    }

    #[inline]
    #[tracing::instrument]
    pub fn early_projection(&self, df_uuid: Uuid, project_list: &[usize]) -> PicachvResult<Uuid> {
        early_projection(&self.arena, df_uuid, project_list)
    }

    #[tracing::instrument]
    pub fn expr_from_args(&self, expr_arg: ExprArgument) -> PicachvResult<Uuid> {
        let expr_arg = expr_arg.argument.ok_or(PicachvError::InvalidOperation(
            "The argument is empty.".into(),
        ))?;

        let expr = Expr::from_args(&self.arena, expr_arg)?;
        let mut expr_arena = rwlock_unlock!(self.arena.expr_arena, write);
        let uuid = expr_arena.insert(expr)?;
        tracing::debug!("expr_from_args: uuid = {uuid}");

        Ok(uuid)
    }

    /// This function will parse the `TransformInfo` and apply the corresponding transformation on
    /// the invovled dataframes so that we can keep sync with the original dataframe since policies
    /// are de-coupled with their data. After succesful application it returns the UUID of the new
    /// dataframe allocated in the arena.
    #[tracing::instrument]
    pub fn execute_epilogue(
        &self,
        df_uuid: Uuid,
        plan_arg: Option<PlanArgument>,
    ) -> PicachvResult<Uuid> {
        match plan_arg {
            Some(plan_arg) => {
                let arg = plan_arg.argument.ok_or(PicachvError::InvalidOperation(
                    "The argument is empty.".into(),
                ))?;

                if let plan_argument::Argument::Transform(_) = arg {
                    let ti = plan_arg
                        .transform_info
                        .ok_or(PicachvError::InvalidOperation(
                            "The transform info is empty.".into(),
                        ))?;

                    return if self.options.enable_profiling {
                        PROFILER.profile(
                            || apply_transform(&self.arena.df_arena, df_uuid, ti, &self.options),
                            "apply_transform".into(),
                        )
                    } else {
                        apply_transform(&self.arena.df_arena, df_uuid, ti, &self.options)
                    };
                }

                let plan = Plan::from_args(&self.arena, arg)?;
                let df_uuid = if self.options.enable_profiling {
                    PROFILER.profile(
                        || plan.check_executor(&self.arena, df_uuid, &self.udfs),
                        "check_executor".into(),
                    )
                } else {
                    plan.check_executor(&self.arena, df_uuid, &self.udfs)
                }?;

                if let Some(ti) = plan_arg.transform_info {
                    if self.options.enable_profiling {
                        PROFILER.profile(
                            || apply_transform(&self.arena.df_arena, df_uuid, ti, &self.options),
                            "apply_transform".into(),
                        )
                    } else {
                        apply_transform(&self.arena.df_arena, df_uuid, ti, &self.options)
                    }
                } else {
                    Ok(df_uuid)
                }
            },
            None => Ok(df_uuid),
        }
    }

    #[tracing::instrument]
    pub fn create_slice(&self, df_uuid: Uuid, range: Range<usize>) -> PicachvResult<Uuid> {
        let mut df_arena = rwlock_unlock!(self.arena.df_arena, write);
        let df = df_arena.get(&df_uuid)?;

        let new_df = df.slice(range)?;
        df_arena.insert(new_df)
    }

    #[tracing::instrument]
    pub fn finalize(&self, df_uuid: Uuid) -> PicachvResult<()> {
        let df_arena = rwlock_unlock!(self.arena.df_arena, read);

        let df = df_arena.get(&df_uuid)?;
        df.finalize()?;

        if self.profiling_enabled() {
            let dump = PROFILER.dump();
            let raw = PROFILER.dump_raw();
            std::fs::write(
                "./profile.log",
                format!("Aggregated:\n{:#?}\nRaw:\n{:#?}", dump, raw),
            )
            .map_err(|e| {
                PicachvError::InvalidOperation(format!("Failed to write profile: {e}").into())
            })?;
        }

        Ok(())
    }

    /// Reify an abstract value of the expression with the given values encoded in the bytes.
    ///
    /// The input values are just a serialized Arrow IPC data represented as record batches.
    #[tracing::instrument]
    pub fn reify_expression(&self, expr_uuid: Uuid, value: &[u8]) -> PicachvResult<()> {
        tracing::debug!("reify_expression: expression uuid = {expr_uuid} ");

        let mut expr_arena = rwlock_unlock!(self.arena.expr_arena, write);
        let expr = expr_arena.get_mut(&expr_uuid).expect("???");
        let expr = match Arc::get_mut(expr) {
            Some(expr) => expr,
            None => {
                return Err(PicachvError::InvalidOperation(
                    "The expression is immutable.".into(),
                ))
            },
        };

        if !expr.needs_reify() {
            return Ok(());
        }

        // Check if it is a column expression.
        if let Expr::Column(_) = expr {
            // Replace the column name with the actual value.
            let idx = usize::from_le_bytes(value.try_into().map_err(|e| {
                PicachvError::InvalidOperation(
                    format!("Failed to convert the value into usize: {e}").into(),
                )
            })?);

            *expr = Expr::Column(ColumnIdent::ColumnId(idx));
        } else if let Expr::Ternary { cond_values, .. } = expr {
            cond_values.replace(value.iter().map(|v| *v != 0).collect());
        } else {
            let f = || {
                // Convert values into the Arrow record batch.
                record_batch_from_bytes(value)
            };
            let rb = if self.options.enable_profiling {
                PROFILER.profile(f, "reify_expression".into())
            } else {
                f()
            }?;

            expr.reify(rb)?;
        }

        Ok(())
    }

    #[inline]
    pub fn id(&self) -> Uuid {
        self.id
    }

    #[tracing::instrument]
    pub fn get_df(&self, df_uuid: Uuid) -> PicachvResult<Arc<PolicyGuardedDataFrame>> {
        let df_arena = rwlock_unlock!(self.arena.df_arena, read);
        df_arena.get(&df_uuid).cloned()
    }
}

/// The definition of our policy monitor.
pub struct PicachvMonitor {
    /// The context map.
    pub(crate) ctx: Arc<RwLock<HashMap<Uuid, Context>>>,
    pub(crate) udfs: Arc<RwLock<HashMap<String, Udf>>>,
}

impl Default for PicachvMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl PicachvMonitor {
    pub fn new() -> Self {
        if let Err(e) = enable_tracing("./picachv.log") {
            match e {
                PicachvError::Already(_) => (),
                _ => panic!("Failed to enable tracing: {e}"),
            }
        }

        PicachvMonitor {
            ctx: Arc::new(RwLock::new(HashMap::new())),
            udfs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn enable_tracing(&self, ctx_id: Uuid, enable: bool) -> PicachvResult<()> {
        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.enable_tracing(enable)
    }

    pub fn enable_profiling(&self, ctx_id: Uuid, enable: bool) -> PicachvResult<()> {
        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.enable_profiling(enable)
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
        tracing::debug!("build_expr");

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
    ) -> PicachvResult<Uuid> {
        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.execute_epilogue(df_uuid, plan_arg)
    }

    pub fn finalize(&self, ctx_id: Uuid, df_uuid: Uuid) -> PicachvResult<()> {
        let mut ctx = rwlock_unlock!(self.ctx, write);
        let ctx = ctx
            .get_mut(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.finalize(df_uuid)
    }
}

pub static MONITOR_INSTANCE: LazyLock<Arc<PicachvMonitor>> =
    LazyLock::new(|| Arc::new(PicachvMonitor::new()));

fn enable_tracing<P: AsRef<Path>>(path: P) -> PicachvResult<()> {
    let log_file = OpenOptions::new().create(true).append(true).open(path)?;

    let debug_log = tracing_subscriber::fmt::layer()
        .with_writer(Arc::new(log_file))
        .with_ansi(false);

    tracing_subscriber::registry()
        .with(debug_log)
        .try_init()
        .map_err(|e| PicachvError::Already(e.to_string().into()))
}
