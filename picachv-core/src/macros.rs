#[macro_export]
macro_rules! policy_unary_transform_label {
    ($name:expr) => {
        $crate::policy::PolicyLabel::PolicyTransform {
            ops: $crate::policy::TransformOps(vec![$crate::policy::TransformType::Unary(
                $crate::policy::UnaryTransformType { name: $name },
            )]),
        }
    };
}

#[macro_export]
macro_rules! policy_binary_transform_label {
    ($name:expr) => {
        $crate::policy::PolicyLabel::PolicyTransform {
            ops: $crate::policy::TransformOps(vec![$name]),
        }
    };

    ($name:expr, $arg:expr) => {
        $crate::policy::PolicyLabel::PolicyTransform {
            ops: $crate::policy::TransformOps(vec![$crate::policy::TransformType::Binary(
                $crate::policy::BinaryTransformType {
                    name: $name.into(),
                    arg: $arg,
                },
            )]),
        }
    };
}

#[macro_export]
macro_rules! policy_agg_label {
    ($how:expr, $size:expr) => {
        $crate::policy::PolicyLabel::PolicyAgg {
            ops: $crate::policy::AggOps(vec![$crate::policy::AggType {
                how: $how,
                group_size: $size,
            }]),
        }
    };
}
