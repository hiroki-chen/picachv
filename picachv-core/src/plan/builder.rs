use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{get_data_argument::DataSource, plan_argument};
use uuid::Uuid;

use crate::{rwlock_unlock, Arenas};

use super::Plan;

// fn format_err(msg: &str, input: &Plan) -> String {
//     format!("{msg}\n\nError originated just after this operation:\n{input:?}")
// }

// /// Returns every error or msg: &str as `ComputeError`.
// /// It also shows the logical plan node where the error
// /// originated.
// macro_rules! raise_err {
//     ($err:expr, $input:expr, $convert:ident) => {{
//         let format_err_outer = |msg: &str| format_err(msg, &$input);

//         let err = $err.wrap_msg(&format_err_outer);

//         InternalLogicPlan::Error {
//             input: Box::new($input.clone()),
//             err: err.into(),
//         }
//         .$convert()
//     }};
// }

// macro_rules! try_delayed {
//     ($fallible:expr, $input:expr, $convert:ident) => {
//         match $fallible {
//             Ok(success) => success,
//             Err(err) => return raise_err!(err, $input, $convert),
//         }
//     };
// }

impl Plan {
    /// Build logical plan from the arguments.
    pub fn from_args(arenas: &Arenas, arg: plan_argument::Argument) -> PicachvResult<Self> {
        use plan_argument::Argument;

        log::debug!("Building logical plan from the arguments {arg:?}");
        let lp_arena = rwlock_unlock!(arenas.lp_arena, write);
        let df_arena = rwlock_unlock!(arenas.df_arena, write);
        match arg {
            Argument::GetData(data_source) => match data_source.data_source {
                Some(data_source) => match data_source {
                    DataSource::FromFile(_) => {
                        Err(PicachvError::ComputeError("Not implemented!".into()))
                    },
                    DataSource::InMemory(memory) => {
                        let df_uuid =
                            Uuid::from_slice_le(memory.df_uuid.as_slice()).map_err(|_| {
                                PicachvError::InvalidOperation(
                                    "The UUID for the dataframe is invalid.".into(),
                                )
                            })?;

                        // Get the policy dataframe from the arena.
                        let df = df_arena.get(&df_uuid)?;
                        let expr_arena = rwlock_unlock!(arenas.expr_arena, read);
                        let selection = memory
                            .pred
                            .as_ref()
                            .cloned()
                            .map(|pred| {
                                let pred_uuid =
                                    Uuid::from_slice_le(pred.as_slice()).map_err(|_| {
                                        PicachvError::InvalidOperation(
                                            "The UUID is invalid.".into(),
                                        )
                                    })?;
                                expr_arena.get(&pred_uuid)
                            })
                            .transpose()?
                            .map(|e| (**e).clone());

                        let projection = if memory.projected_list.is_empty() {
                            None
                        } else {
                            let proj_list = memory
                                .projected_list
                                .into_iter()
                                .map(|e| e as usize)
                                .collect::<Vec<_>>();

                            Some(proj_list)
                        };

                        Ok(Plan::DataFrameScan {
                            schema: df.schema.clone(),
                            output_schema: None,
                            projection,
                            selection,
                        })
                    },
                },
                None => Err(PicachvError::InvalidOperation(
                    "The data source is empty; It must not be empty".into(),
                )),
            },
            Argument::Projection(proj_arg) => {
                let expr_arena = rwlock_unlock!(arenas.expr_arena, read);

                let input_uuid = Uuid::from_slice_le(proj_arg.input_uuid.as_slice())
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
                let input = lp_arena.get(&input_uuid)?;
                // let schema_uuid = Uuid::from_slice_le(proj_arg.schema_uuid.as_slice())
                //     .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
                // let df = df_arena.get(&schema_uuid)?;

                // For each expression in the list, we can get the expression from the arena
                // by using the UUID since we have already stored the expression in the arena
                // when the caller is constructing the physical expression and physical plan,
                // and then we can construct the logical plan.
                let proj_list = proj_arg
                    .expression
                    .into_iter()
                    .map(|expr| {
                        let expr_uuid = Uuid::from_slice_le(expr.as_slice()).map_err(|_| {
                            PicachvError::InvalidOperation("The UUID is invalid.".into())
                        })?;
                        let expr = expr_arena.get(&expr_uuid)?;
                        Ok((**expr).clone())
                    })
                    .collect::<PicachvResult<Vec<_>>>()?;

                Ok(Plan::Projection {
                    input: Box::new((**input).clone()),
                    schema: Default::default(),
                    expression: proj_list,
                })
            },

            Argument::Union(union_arg) => {
                let left_uuid = Uuid::from_slice_le(union_arg.left_uuid.as_slice())
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
                let right_uuid = Uuid::from_slice_le(union_arg.right_uuid.as_slice())
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
                let schema_uuid = Uuid::from_slice_le(union_arg.schema_uuid.as_slice())
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
                let left = lp_arena.get(&left_uuid)?;
                let right = lp_arena.get(&right_uuid)?;
                let df = df_arena.get(&schema_uuid)?;

                Ok(Plan::Union {
                    input_left: Box::new((**left).clone()),
                    input_right: Box::new((**right).clone()),
                    schema: df.schema.clone(),
                })
            },

            Argument::Select(select_arg) => {
                let expr_arena = rwlock_unlock!(arenas.expr_arena, read);

                let input_uuid = Uuid::from_slice_le(select_arg.input_uuid.as_slice())
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
                let input = lp_arena.get(&input_uuid)?;

                let expr_uuid = Uuid::from_slice_le(select_arg.pred_uuid.as_slice())
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
                let expr = expr_arena.get(&expr_uuid)?;

                Ok(Plan::Select {
                    input: Box::new((**input).clone()),
                    predicate: (**expr).clone(),
                })
            },

            _ => Err(PicachvError::InvalidOperation(
                "The operation is not supported.".into(),
            )),
        }
    }
}
