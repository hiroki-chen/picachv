use picachv_error::{picachv_bail, picachv_ensure, PicachvError, PicachvResult};
use picachv_message::column_specifier::Column;
use picachv_message::{expr_argument, AggExpr, ApplyExpr, TernaryExpr};
use uuid::Uuid;

use super::AExpr;
use crate::expr::ColumnIdent;
use crate::udf::Udf;
use crate::Arenas;

impl AExpr {
    /// Build expression from the arguments.
    pub fn from_args(arenas: &Arenas, arg: expr_argument::Argument) -> PicachvResult<Self> {
        use expr_argument::Argument;

        #[cfg(feature = "trace")]
        tracing::debug!("Building expression from the arguments {arg:?}");
        let expr_arena = arenas.expr_arena.read();
        match arg {
            Argument::Column(expr) => match expr.column {
                Some(column) => match column.column {
                    Some(column) => match column {
                        Column::ColumnIndex(idx) => {
                            Ok(AExpr::Column(ColumnIdent::ColumnId(idx as usize)))
                        },
                        Column::ColumnName(name) => Ok(AExpr::Column(ColumnIdent::ColumnName(name))),
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

                Ok(AExpr::BinaryExpr {
                    left: left_uuid,
                    op,
                    right: right_uuid,
                    values: None, // must be reified later.
                })
            },
            Argument::Count(_) => Ok(AExpr::Count),
            Argument::Literal(_) => Ok(AExpr::Literal),
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

                Ok(AExpr::Apply {
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
                        picachv_message::GroupByMethod::Sum => Ok(AExpr::Agg {
                            expr: crate::expr::AAggExpr::Sum(uuid),
                            values: None,
                        }),
                        picachv_message::GroupByMethod::Mean => Ok(AExpr::Agg {
                            expr: crate::expr::AAggExpr::Mean(uuid),
                            values: None,
                        }),
                        picachv_message::GroupByMethod::Max => Ok(AExpr::Agg {
                            expr: crate::expr::AAggExpr::Max {
                                input: uuid,
                                propagate_nans: true,
                            },
                            values: None,
                        }),
                        picachv_message::GroupByMethod::Min => Ok(AExpr::Agg {
                            expr: crate::expr::AAggExpr::Min {
                                input: uuid,
                                propagate_nans: true,
                            },
                            values: None,
                        }),
                        picachv_message::GroupByMethod::Len => Ok(AExpr::Agg {
                            expr: crate::expr::AAggExpr::Count(uuid, true),
                            values: None,
                        }),

                        picachv_message::GroupByMethod::First => Ok(AExpr::Agg {
                            expr: crate::expr::AAggExpr::First(uuid),
                            values: None,
                        }),
                        _ => todo!(),
                    },
                    Err(e) => picachv_bail!(ComputeError: "{e}"),
                }
            },

            Argument::Ternary(TernaryExpr {
                cond_uuid,
                then_uuid,
                else_uuid,
            }) => {
                let cond_uuid = Uuid::from_slice_le(&cond_uuid)
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
                let then_uuid = Uuid::from_slice_le(&then_uuid)
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
                let else_uuid = Uuid::from_slice_le(&else_uuid)
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;

                picachv_ensure!(
                    expr_arena.contains_key(&cond_uuid)
                        && expr_arena.contains_key(&then_uuid)
                        && expr_arena.contains_key(&else_uuid),
                    InvalidOperation: "The UUID is invalid."
                );

                Ok(AExpr::Ternary {
                    cond: cond_uuid,
                    then: then_uuid,
                    otherwise: else_uuid,
                    cond_values: None, // reified later.
                })
            },

            _ => todo!(),
        }
    }
}
