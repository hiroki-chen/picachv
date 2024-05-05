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
macro_rules! policy_binary_transform_label {
    ($name:expr, $arg:expr) => {
        $crate::policy::PolicyLabel::PolicyTransform {
            ops: TransformOps(::std::collections::HashSet::from_iter(
                vec![$crate::policy::TransformType::Binary(
                    $crate::policy::BinaryTransformType {
                        name: $name,
                        arg: $arg,
                    },
                )]
                .into_iter(),
            )),
        }
    };
}
