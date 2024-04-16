use std::fmt::Debug;

/// A trait for a lattice. A lattice is a partially ordered set where every pair of elements has a
/// unique least upper bound (join) and greatest lower bound (meet).
///
/// # Examples
///
/// ```
/// use policy_core::lattice::Lattice;
///
/// #[derive(Clone, Copy, Debug)]
/// enum LatticeEnum {
///     Top,
///     Bottom,
/// }
///
/// impl Lattice<LatticeEnum> for LatticeEnum {
///     fn join(&self, other: &Self) -> Self {
///         match (self, other) {
///             (LatticeEnum::Top, _) => LatticeEnum::Top,
///             (_, LatticeEnum::Top) => LatticeEnum::Top,
///             (LatticeEnum::Bottom, _) => *other,
///             (_, LatticeEnum::Bottom) => *self,
///         }
///     }
///
///     fn meet(&self, other: &Self) -> Self {
///         match (self, other) {
///             (LatticeEnum::Bottom, _) => LatticeEnum::Bottom,
///             (_, LatticeEnum::Bottom) => LatticeEnum::Bottom,
///             (LatticeEnum::Top, _) => *other,
///             (_, LatticeEnum::Top) => *self,
///         }
///     }
///
///     fn top() -> Self {
///         LatticeEnum::Top
///     }
///
///     fn bottom() -> Self {
///         LatticeEnum::Bottom
///    }
///
pub trait Lattice: PartialEq + Clone + Debug {
    /// Returns the join of two elements.
    fn join(&self, other: &Self) -> Self;
    /// Returns the meet of two elements.
    fn meet(&self, other: &Self) -> Self;
    /// Returns the top element of the lattice.
    fn top() -> Self;
    /// Returns the bottom element of the lattice.
    fn bottom() -> Self;
    /// Returns true if `self` is less than or equal to `other`.
    fn flowsto(&self, other: &Self) -> bool {
        self.join(other) == *other
    }
}
