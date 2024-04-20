use picachv_error::{PicachvError, PicachvResult};
use picachv_message::{expr_argument, plan_argument};
use uuid::Uuid;

use crate::{
    callback::{Callback, Caller},
    expr::Expr,
    rwlock_unlock, Arenas,
};

use super::Plan;

fn format_err(msg: &str, input: &Plan) -> String {
    format!("{msg}\n\nError originated just after this operation:\n{input:?}")
}

/// Returns every error or msg: &str as `ComputeError`.
/// It also shows the logical plan node where the error
/// originated.
macro_rules! raise_err {
    ($err:expr, $input:expr, $convert:ident) => {{
        let format_err_outer = |msg: &str| format_err(msg, &$input);

        let err = $err.wrap_msg(&format_err_outer);

        InternalLogicPlan::Error {
            input: Box::new($input.clone()),
            err: err.into(),
        }
        .$convert()
    }};
}

macro_rules! try_delayed {
    ($fallible:expr, $input:expr, $convert:ident) => {
        match $fallible {
            Ok(success) => success,
            Err(err) => return raise_err!(err, $input, $convert),
        }
    };
}

impl Plan {
    /// Build logical plan from the arguments.
    pub(crate) fn from_args(
        arenas: &Arenas,
        arg: plan_argument::Argument,
        cb: Callback,
    ) -> PicachvResult<Self> {
        use plan_argument::Argument;

        let lp_arena = rwlock_unlock!(arenas.lp_arena, write);
        let schema_arena = rwlock_unlock!(arenas.schema_arena, write);
        match arg {
            Argument::GetData(data_source) => match data_source.data_source {
                Some(data_source) => todo!(),
                None => Err(PicachvError::InvalidOperation(
                    "The data source is empty; It must not be empty".into(),
                )),
            },

            Argument::Union(union_arg) => {
                let (left_uuid, right_uuid, schema_uuid) = {
                    let left_uuid: [u8; 16] = union_arg.left_uuid.try_into().map_err(|_| {
                        PicachvError::InvalidOperation("The UUID is invalid.".into())
                    })?;
                    let right_uuid: [u8; 16] = union_arg.right_uuid.try_into().map_err(|_| {
                        PicachvError::InvalidOperation("The UUID is invalid.".into())
                    })?;
                    let schema_uuid: [u8; 16] = union_arg.schema_uuid.try_into().map_err(|_| {
                        PicachvError::InvalidOperation("The UUID is invalid.".into())
                    })?;

                    (
                        Uuid::from_bytes(left_uuid),
                        Uuid::from_bytes(right_uuid),
                        Uuid::from_bytes(schema_uuid),
                    )
                };
                let left = lp_arena.get(&left_uuid)?;
                let right = lp_arena.get(&right_uuid)?;
                let schema = schema_arena.get(&schema_uuid)?;

                Ok(Plan::Union {
                    input_left: Box::new(left.as_ref().clone()),
                    input_right: Box::new(right.as_ref().clone()),
                    schema: schema.clone(),
                    cb: Caller::new(cb),
                })
            },
            _ => todo!(),
        }
    }
}

impl Expr {
    /// Build expression from the arguments.
    pub(crate) fn from_args(arenas: &Arenas, arg: expr_argument::Argument) -> PicachvResult<Self> {
        match arg {
            _ => todo!(),
        }
    }
}
