use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use picachv_message::column_specifier::Column;
use picachv_message::{expr_argument, AggExpr, ApplyExpr};
use uuid::Uuid;

use super::Expr;
use crate::expr::ColumnIdent;
use crate::udf::Udf;
use crate::{rwlock_unlock, Arenas};

impl Expr {
    /// Build expression from the arguments.
    pub fn from_args(arenas: &Arenas, arg: expr_argument::Argument) -> PicachvResult<Self> {
        use expr_argument::Argument;

        tracing::debug!("Building expression from the arguments {arg:?}");
        let expr_arena = rwlock_unlock!(arenas.expr_arena, read);
        match arg {
            Argument::Column(expr) => match expr.column {
                Some(column) => match column.column {
                    Some(column) => match column {
                        Column::ColumnIndex(idx) => {
                            Ok(Expr::Column(ColumnIdent::ColumnId(idx as usize)))
                        },
                        Column::ColumnName(name) => Ok(Expr::Column(ColumnIdent::ColumnName(name))),
                    },
                    None => Err(PicachvError::InvalidOperation(
                        "The column is empty.".into(),
                    )),
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

                picachv_ensure!(
                    expr_arena.contains_key(&left_uuid) &&
                    expr_arena.contains_key(&right_uuid),

                    InvalidOperation: "The UUID is invalid."
                );

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
                    left: left_uuid,
                    op,
                    right: right_uuid,
                    values: None, // must be reified later.
                })
            },
            Argument::Literal(_) => Ok(Expr::Literal),
            Argument::Apply(ApplyExpr { input_uuids, name }) => {
                let args = input_uuids
                    .into_iter()
                    .map(|uuid| {
                        Uuid::from_slice_le(&uuid).map_err(|_| {
                            PicachvError::InvalidOperation("The UUID is invalid.".into())
                        })
                    })
                    .collect::<PicachvResult<Vec<_>>>()?;

                for uuid in &args {
                    picachv_ensure!(
                        expr_arena.contains_key(uuid),
                        InvalidOperation: "The UUID is invalid."
                    );
                }

                Ok(Expr::Apply {
                    udf_desc: Udf { name },
                    args,
                    values: None, // must be reified later.
                })
            },

            Argument::Agg(AggExpr { input_uuid, method }) => {
                let uuid = Uuid::from_slice_le(&input_uuid)
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;

                picachv_ensure!(
                    expr_arena.contains_key(&uuid),
                    InvalidOperation: "The UUID is invalid."
                );

                match picachv_message::GroupByMethod::try_from(method) {
                    Ok(how) => match how {
                        picachv_message::GroupByMethod::Sum => Ok(Expr::Agg {
                            expr: crate::expr::AggExpr::Sum(uuid),
                            values: None,
                        }),
                        _ => todo!(),
                    },
                    Err(e) => picachv_bail!(ComputeError: "{e}"),
                }
            },

            _ => todo!(),
        }
    }
}
