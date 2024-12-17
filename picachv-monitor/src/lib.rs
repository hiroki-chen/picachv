use std::fmt;
use std::path::Path;
use std::sync::{Arc, LazyLock};

use ahash::{HashMap, HashMapExt};
use picachv_core::dataframe::{apply_transform, PolicyGuardedDataFrame};
use picachv_core::expr::{AExpr, ColumnIdent};
use picachv_core::io::{BinIo, JsonIO};
use picachv_core::plan::{early_projection, Plan};
use picachv_core::profiler::PROFILER;
use picachv_core::udf::Udf;
use picachv_core::{get_new_uuid, record_batch_from_bytes, Arenas};
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{plan_argument, ContextOptions, ExprArgument, PlanArgument};
use prost::Message;
use spin::RwLock;
use uuid::Uuid;

/// An activate context for the data analysis.
pub struct Context {
    /// The context ID.
    id: Uuid,
    /// A place for storing objects.
    arena: Arenas,
    /// Context options.
    pub(crate) options: Arc<RwLock<ContextOptions>>,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context").field("id", &self.id).finish()
    }
}

impl Context {
    #[inline]
    pub fn enable_tracing(&self, enable: bool) -> PicachvResult<()> {
        self.options.write().enable_tracing = enable;

        Ok(())
    }

    #[inline]
    pub fn tracing_enabled(&self) -> bool {
        self.options.read().enable_tracing
    }

    #[inline]
    pub fn enable_profiling(&self, enable: bool) -> PicachvResult<()> {
        self.options.write().enable_profiling = enable;

        Ok(())
    }

    #[inline]
    pub fn profiling_enabled(&self) -> bool {
        self.options.read().enable_profiling
    }

    #[inline]
    pub fn new(id: Uuid) -> Self {
        Context {
            id,
            arena: Arenas::new(),
            options: Arc::new(RwLock::new(ContextOptions::default())),
        }
    }

    #[inline]
    #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn register_policy_dataframe(&self, df: PolicyGuardedDataFrame) -> PicachvResult<Uuid> {
        let mut df_arena = self.arena.df_arena.write();
        df_arena.insert(df)
    }

    #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn register_policy_dataframe_json<P: AsRef<Path> + fmt::Debug>(
        &self,
        path: P,
    ) -> PicachvResult<Uuid> {
        let df = PolicyGuardedDataFrame::from_json(path.as_ref())?;
        self.register_policy_dataframe(df)
    }

    #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn register_policy_dataframe_bin<P: AsRef<Path> + fmt::Debug>(
        &self,
        path: P,
    ) -> PicachvResult<Uuid> {
        let df = PolicyGuardedDataFrame::from_bytes(path.as_ref())?;
        self.register_policy_dataframe(df)
    }

    #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn register_policy_dataframe_from_row_group<P: AsRef<Path> + fmt::Debug>(
        &self,
        path: P,
        projection: &[usize],
        selection: Option<&[bool]>,
        row_group: usize,
    ) -> PicachvResult<Uuid> {
        let df =
            PolicyGuardedDataFrame::from_parquet_row_group(path, projection, selection, row_group)?;

        self.register_policy_dataframe(df)
    }

    #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn register_policy_dataframe_parquet<P: AsRef<Path> + fmt::Debug>(
        &self,
        path: P,
        projection: &[usize],
        selection: Option<&[bool]>,
    ) -> PicachvResult<Uuid> {
        let df = if self.options.read().enable_profiling {
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
    #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn early_projection(&self, df_uuid: Uuid, project_list: &[usize]) -> PicachvResult<Uuid> {
        early_projection(&self.arena, df_uuid, project_list)
    }

    #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn expr_from_args(&self, expr_arg: ExprArgument) -> PicachvResult<Uuid> {
        let expr_arg = expr_arg.argument.ok_or(PicachvError::InvalidOperation(
            "The argument is empty.".into(),
        ))?;

        let expr = AExpr::from_args(&self.arena, expr_arg)?;
        let mut expr_arena = self.arena.expr_arena.write();
        let uuid = expr_arena.insert(expr)?;

        #[cfg(feature = "trace")]
        tracing::debug!("expr_from_args: uuid = {uuid}");

        Ok(uuid)
    }

    /// This function will parse the `TransformInfo` and apply the corresponding transformation on
    /// the invovled dataframes so that we can keep sync with the original dataframe since policies
    /// are de-coupled with their data. After succesful application it returns the UUID of the new
    /// dataframe allocated in the arena.
    #[cfg_attr(feature = "trace", tracing::instrument)]
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

                    return if self.options.read().enable_profiling {
                        PROFILER.profile(
                            || {
                                apply_transform(
                                    &self.arena.df_arena,
                                    df_uuid,
                                    ti,
                                    &self.options.read().clone(),
                                )
                            },
                            "apply_transform".into(),
                        )
                    } else {
                        apply_transform(
                            &self.arena.df_arena,
                            df_uuid,
                            ti,
                            &self.options.read().clone(),
                        )
                    };
                }

                let plan = Plan::from_args(&self.arena, arg)?;
                let df_uuid = if self.options.read().enable_profiling {
                    PROFILER.profile(
                        || {
                            plan.check_executor(
                                &self.arena,
                                df_uuid,
                                &Default::default(),
                                &self.options.read().clone(),
                            )
                        },
                        "check_executor".into(),
                    )
                } else {
                    plan.check_executor(
                        &self.arena,
                        df_uuid,
                        &Default::default(),
                        &self.options.read().clone(),
                    )
                }?;

                if let Some(ti) = plan_arg.transform_info {
                    if self.options.read().enable_profiling {
                        PROFILER.profile(
                            || {
                                apply_transform(
                                    &self.arena.df_arena,
                                    df_uuid,
                                    ti,
                                    &self.options.read().clone(),
                                )
                            },
                            "apply_transform".into(),
                        )
                    } else {
                        apply_transform(
                            &self.arena.df_arena,
                            df_uuid,
                            ti,
                            &self.options.read().clone(),
                        )
                    }
                } else {
                    Ok(df_uuid)
                }
            },
            None => Ok(df_uuid),
        }
    }

    #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn create_slice(&self, df_uuid: Uuid, sel_vec: &[u32]) -> PicachvResult<Uuid> {
        let mut df_arena = self.arena.df_arena.write();
        let df = df_arena.get(&df_uuid)?;
        let sel_vec = sel_vec.into_iter().map(|e| *e as usize).collect::<Vec<_>>();

        let new_df = df.new_from_slice(&sel_vec)?;
        df_arena.insert(new_df)
    }

    #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn finalize(&self, df_uuid: Uuid) -> PicachvResult<()> {
        let df_arena = self.arena.df_arena.read();

        let df = df_arena.get(&df_uuid)?;

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

        df.finalize()
    }

    #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn select_group(&self, df_uuid: Uuid, hashes: &[u64]) -> PicachvResult<Uuid> {
        let mut df_arena = self.arena.df_arena.write();
        let df = df_arena.get(&df_uuid)?;

        let new_df = df.select_group(hashes);
        println!("new_df.is_err() = {}", new_df.is_err());
        df_arena.insert(new_df?)
    }

    /// Reify an abstract value of the expression with the given values encoded in the bytes.
    ///
    /// The input values are just a serialized Arrow IPC data represented as record batches.
    // #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn reify_expression(&self, expr_uuid: Uuid, value: &[u8]) -> PicachvResult<()> {
        let expr_arena = self.arena.expr_arena.read();

        let expr = unsafe {
            let expr = expr_arena.get(&expr_uuid)?;

            if Arc::strong_count(expr) > 1 {
                panic!("trying to reify an expression in use. This is internal logic error.");
            }

            &mut *(Arc::as_ptr(expr) as *mut AExpr)
        };

        if !expr.needs_reify() {
            return Ok(());
        }

        // Check if it is a column expression.
        if let AExpr::Column(_) = expr {
            // Replace the column name with the actual value.
            let idx = usize::from_le_bytes(value.try_into().map_err(|_| {
                PicachvError::InvalidOperation(
                    format!("Failed to convert the value into usize: {value:?}").into(),
                )
            })?);

            *expr = AExpr::Column(ColumnIdent::ColumnId(idx));
        } else if let AExpr::Ternary { cond_values, .. } = expr {
            cond_values.replace(value.iter().map(|v| *v != 0).collect::<Vec<_>>().into());
        } else {
            let f = || {
                // Convert values into the Arrow record batch.
                record_batch_from_bytes(value)
            };
            let rb = if self.options.read().enable_profiling {
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

    #[cfg_attr(feature = "trace", tracing::instrument)]
    pub fn get_df(&self, df_uuid: Uuid) -> PicachvResult<Arc<PolicyGuardedDataFrame>> {
        let df_arena = self.arena.df_arena.read();
        df_arena.get(&df_uuid).cloned()
    }
}

/// The definition of our policy monitor.
pub struct PicachvMonitor {
    /// The context map.
    pub(crate) ctx: HashMap<Uuid, Context>,
    #[allow(dead_code)]
    pub(crate) udfs: HashMap<String, Udf>,
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
            ctx: HashMap::new(),
            udfs: HashMap::new(),
        }
    }

    pub fn enable_tracing(&self, ctx_id: Uuid, enable: bool) -> PicachvResult<()> {
        let ctx = self
            .ctx
            .get(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.enable_tracing(enable)
    }

    pub fn enable_profiling(&self, ctx_id: Uuid, enable: bool) -> PicachvResult<()> {
        let ctx = self
            .ctx
            .get(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.enable_profiling(enable)
    }

    pub fn get_ctx(&self) -> &HashMap<Uuid, Context> {
        &self.ctx
    }

    /// Opens a new context.
    pub fn open_new(&mut self) -> PicachvResult<Uuid> {
        let uuid = get_new_uuid();
        let ctx = Context::new(get_new_uuid());

        self.ctx.insert(uuid, ctx);

        Ok(uuid)
    }

    pub fn build_expr(&self, ctx_id: Uuid, expr_arg: &[u8]) -> PicachvResult<Uuid> {
        #[cfg(feature = "trace")]
        tracing::debug!("build_expr");

        let ctx = self
            .ctx
            .get(&ctx_id)
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
        let ctx = self
            .ctx
            .get(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.execute_epilogue(df_uuid, plan_arg)
    }

    pub fn finalize(&self, ctx_id: Uuid, df_uuid: Uuid) -> PicachvResult<()> {
        let ctx = self
            .ctx
            .get(&ctx_id)
            .ok_or_else(|| PicachvError::InvalidOperation("The context does not exist.".into()))?;

        ctx.finalize(df_uuid)
    }
}

pub static MONITOR_INSTANCE: LazyLock<Arc<RwLock<PicachvMonitor>>> =
    LazyLock::new(|| Arc::new(RwLock::new(PicachvMonitor::new())));

#[cfg(feature = "trace")]
fn enable_tracing<P: AsRef<Path>>(path: P) -> PicachvResult<()> {
    use std::fs::OpenOptions;

    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let log_file = OpenOptions::new().create(true).append(true).open(path)?;
    let debug_log = tracing_subscriber::fmt::layer()
        .with_writer(Arc::new(log_file))
        .with_ansi(false);

    tracing_subscriber::registry()
        .with(debug_log)
        .try_init()
        .map_err(|e| PicachvError::Already(e.to_string().into()))
}

#[cfg(not(feature = "trace"))]
fn enable_tracing<P: AsRef<Path>>(_: P) -> PicachvResult<()> {
    Ok(())
}
