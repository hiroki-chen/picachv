use picachv_error::{PicachvError, PicachvResult};
use picachv_message::column_expr::Column;
use picachv_message::{expr_argument, ApplyExpr};
use uuid::Uuid;

use super::Expr;
use crate::udf::Udf;
use crate::{rwlock_unlock, Arenas};

impl Expr {
    /// Build expression from the arguments.
    pub fn from_args(arenas: &Arenas, arg: expr_argument::Argument) -> PicachvResult<Self> {
        use expr_argument::Argument;

        log::debug!("Building expression from the arguments {arg:?}");
        let expr_arena = rwlock_unlock!(arenas.expr_arena, read);
        match arg {
            Argument::Column(expr) => match expr.column {
                Some(column) => match column {
                    Column::ColumnId(_) => Err(PicachvError::InvalidOperation(
                        "The column ID is not supported.".into(),
                    )),
                    Column::ColumnNameSpecifier(name) => Ok(Expr::Column(name.column_name)),
                },
                None => Err(PicachvError::InvalidOperation(
                    "The column is empty.".into(),
                )),
            },
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
                    left: Box::new((**lhs).clone()),
                    op,
                    right: Box::new((**rhs).clone()),
                })
            },
            Argument::Literal(_) => Ok(Expr::Literal),
            Argument::Apply(ApplyExpr { input_uuids, name }) => {
                let args = input_uuids
                    .into_iter()
                    .map(|e| {
                        let uuid = Uuid::from_slice_le(&e).map_err(|_| {
                            PicachvError::InvalidOperation("The UUID is invalid.".into())
                        })?;
                        let expr = expr_arena.get(&uuid)?;
                        Ok(Box::new((**expr).clone()))
                    })
                    .collect::<PicachvResult<Vec<_>>>()?;

                Ok(Expr::Apply {
                    udf_desc: Udf { name },
                    args,
                    values: None, // must be reified later.
                })
            },

            _ => todo!(),
        }
    }
}
