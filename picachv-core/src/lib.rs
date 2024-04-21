use std::sync::{Arc, RwLock};

use arena::Arena;
use callback::Caller;
use expr::{Expr, ExprArena};
use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{ExprArgument, PlanArgument};
use plan::{Plan, PlanArena};
use polars_core::schema::Schema;
use uuid::Uuid;

pub mod arena;
pub mod callback;
pub mod constants;
pub mod dataframe;
pub mod effects;
pub mod expr;
pub mod macros;
pub mod plan;
pub mod policy;

pub struct Arenas {
    pub lp_arena: Arc<RwLock<PlanArena>>,
    pub expr_arena: Arc<RwLock<ExprArena>>,
    pub schema_arena: Arc<RwLock<Arena<Schema>>>,
}

impl Arenas {
    pub fn new() -> Self {
        Arenas {
            lp_arena: Arc::new(RwLock::new(PlanArena::new())),
            expr_arena: Arc::new(RwLock::new(ExprArena::new())),
            schema_arena: Arc::new(RwLock::new(Arena::new())),
        }
    }

    pub fn build_lp(&self, arg: PlanArgument, cb: Caller) -> PicachvResult<Uuid> {
        let arg = arg.argument.ok_or(PicachvError::InvalidOperation(
            "The argument is empty.".into(),
        ))?;

        let lp = Plan::from_args(self, arg, cb)?;

        let mut lock = rwlock_unlock!(self.lp_arena, write);
        lock.insert(lp)
    }

    pub fn build_expr(&self, arg: ExprArgument) -> PicachvResult<Uuid> {
        let arg = arg.argument.ok_or(PicachvError::InvalidOperation(
            "The argument is empty.".into(),
        ))?;

        let expr = Expr::from_args(self, arg)?;

        let mut lock = rwlock_unlock!(self.expr_arena, write);
        lock.insert(expr)
    }
}

pub fn get_new_uuid() -> Uuid {
    Uuid::new_v4()
}

#[cfg(test)]
mod tests {}
