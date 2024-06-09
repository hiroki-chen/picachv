#[macro_export]
macro_rules! rwlock_unlock {
    ($lock:expr, $how:ident) => {
        match $lock.$how() {
            Ok(guard) => guard,
            Err(err) => {
                return Err(picachv_error::PicachvError::ComputeError(
                    err.to_string().into(),
                ))
            },
        }
    };
}

#[macro_export]
macro_rules! policy_unary_transform_label {
    ($name:expr) => {
        $crate::policy::PolicyLabel::PolicyTransform {
            ops: $crate::policy::TransformOps(::std::collections::HashSet::from_iter(
                vec![$crate::policy::TransformType::Unary(
                    $crate::policy::UnaryTransformType { name: $name },
                )]
                .into_iter(),
            )),
        }
    };
}

#[macro_export]
macro_rules! policy_binary_transform_label {
    ($name:expr) => {
        $crate::policy::PolicyLabel::PolicyTransform {
            ops: $crate::policy::TransformOps(::std::collections::HashSet::from_iter(
                vec![$name].into_iter(),
            )),
        }
    };

    ($name:expr, $arg:expr) => {
        $crate::policy::PolicyLabel::PolicyTransform {
            ops: $crate::policy::TransformOps(::std::collections::HashSet::from_iter(
                vec![$crate::policy::TransformType::Binary(
                    $crate::policy::BinaryTransformType {
                        name: $name.into(),
                        arg: $arg,
                    },
                )]
                .into_iter(),
            )),
        }
    };
}

#[macro_export]
macro_rules! policy_agg_label {
    ($how:expr, $size:expr) => {
        $crate::policy::PolicyLabel::PolicyAgg {
            ops: $crate::policy::AggOps(::std::collections::HashSet::from_iter(
                vec![$crate::policy::AggType {
                    how: $how,
                    group_size: $size,
                }]
                .into_iter(),
            )),
        }
    };
}
