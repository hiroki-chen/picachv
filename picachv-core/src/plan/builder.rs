use picachv_error::{PicachvError, PicachvResult};
use picachv_message::get_data_argument::DataSource;
use picachv_message::{plan_argument, AggregateArgument};
use uuid::Uuid;

use super::Plan;
use crate::{rwlock_unlock, Arenas};

impl Plan {
    /// Build logical plan from the arguments.
    pub fn from_args(arenas: &Arenas, arg: plan_argument::Argument) -> PicachvResult<Self> {
        use plan_argument::Argument;

        log::debug!("Building logical plan from the arguments {arg:?}");
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
                        let selection = memory
                            .pred
                            .as_ref()
                            .cloned()
                            .map(|pred| {
                                Uuid::from_slice_le(pred.as_slice()).map_err(|_| {
                                    PicachvError::InvalidOperation("The UUID is invalid.".into())
                                })
                            })
                            .transpose()?;

                        let projection = if memory.projected_list.is_empty() {
                            None
                        } else {
                            let proj_list = memory.projected_list.into_iter().collect::<Vec<_>>();

                            Some(proj_list)
                        };

                        Ok(Plan::DataFrameScan {
                            schema: df.schema.clone(),
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
                // For each expression in the list, we can get the expression from the arena
                // by using the UUID since we have already stored the expression in the arena
                // when the caller is constructing the physical expression and physical plan,
                // and then we can construct the logical plan.
                let proj_list = proj_arg
                    .expression
                    .into_iter()
                    .map(|expr| {
                        Uuid::from_slice_le(expr.as_slice()).map_err(|_| {
                            PicachvError::InvalidOperation("The UUID is invalid.".into())
                        })
                    })
                    .collect::<PicachvResult<Vec<_>>>()?;

                Ok(Plan::Projection {
                    expression: proj_list,
                })
            },

            Argument::Select(select_arg) => {
                let expr_uuid = Uuid::from_slice_le(select_arg.pred_uuid.as_slice())
                    .map_err(|_| PicachvError::InvalidOperation("The UUID is invalid.".into()))?;
                Ok(Plan::Select {
                    predicate: expr_uuid,
                })
            },

            Argument::Aggregate(AggregateArgument {
                keys,
                aggs_uuid,
                maintain_order,
                group_by_proxy,
                output_schema,
            }) => {
                let keys = keys
                    .into_iter()
                    .map(|e| {
                        Uuid::from_slice_le(e.as_slice()).map_err(|_| {
                            PicachvError::InvalidOperation("The UUID is invalid.".into())
                        })
                    })
                    .collect::<PicachvResult<Vec<_>>>()?;
                let aggs = aggs_uuid
                    .into_iter()
                    .map(|e| {
                        Uuid::from_slice_le(e.as_slice()).map_err(|_| {
                            PicachvError::InvalidOperation("The UUID is invalid.".into())
                        })
                    })
                    .collect::<PicachvResult<Vec<_>>>()?;

                Ok(Plan::Aggregation {
                    keys,
                    aggs,
                    maintain_order,
                    gb_proxy: group_by_proxy.ok_or(PicachvError::InvalidOperation(
                        "The group by proxy is empty.".into(),
                    ))?,
                    output_schema,
                })
            },
        }
    }
}
