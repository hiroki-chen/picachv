use picachv_error::{PicachvError, PicachvResult};
use picachv_message::expr_argument;
use uuid::Uuid;

use crate::{rwlock_unlock, Arenas};

use super::Expr;

impl Expr {
    /// Build expression from the arguments.
    pub fn from_args(arenas: &Arenas, arg: expr_argument::Argument) -> PicachvResult<Self> {
        use expr_argument::Argument;

        log::debug!("Building expression from the arguments {arg:?}");
        let expr_arena = rwlock_unlock!(arenas.expr_arena, read);
        match arg {
            Argument::Column(expr) => Ok(Expr::Column(expr.column_id as _)),
            Argument::Binary(expr) => {
                let left_uuid = Uuid::from_slice_le(&expr.left_uuid)
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
                let right_uuid = Uuid::from_slice_le(&expr.right_uuid)
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;

                let lhs = expr_arena.get(&left_uuid)?;
                let rhs = expr_arena.get(&right_uuid)?;
                let op = expr
                    .op
                    .ok_or(PicachvError::InvalidOperation(
                        "Empty operator found".into(),
                    ))?
                    .operator
                    .ok_or(PicachvError::InvalidOperation(
                        "Empty operator found".into(),
                    ))?;

                Ok(Expr::BinaryExpr {
                    left: Box::new((*lhs).clone()),
                    op,
                    right: Box::new((*rhs).clone()),
                })
            },
            // Argument::Filter(expr) => {
            //     let input_uuid = Uuid::from_slice_le(&expr.input_uuid)
            //         .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
            //     let filter_uuid = Uuid::from_slice_le(&expr.filter_uuid)
            //         .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;

            //     // TODO: Do we really need `input`?
            //     let input = expr_arena.get(&input_uuid)?;
            //     let filter = expr_arena.get(&filter_uuid)?;

            //     Ok(Expr::Filter {
            //         input: Box::new(input.clone()),
            //         filter: Box::new(filter.clone()),
            //     })
            // },
            _ => todo!(),
        }
    }
}
