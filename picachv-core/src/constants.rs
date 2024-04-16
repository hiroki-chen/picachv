#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Hash)]
pub enum QuantileInterpolOptions {
    #[default]
    Nearest,
    Lower,
    Higher,
    Midpoint,
    Linear,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum GroupByMethod {
    Min,
    NanMin,
    Max,
    NanMax,
    Median,
    Mean,
    First,
    Last,
    Sum,
    Groups,
    NUnique,
    Quantile(f64, QuantileInterpolOptions),
    Count { include_nulls: bool },
    Implode,
    Std(u8),
    Var(u8),
}
