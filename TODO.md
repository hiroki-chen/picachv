# TODOs

[x] Implement `fold_on_groups`.
[x] Implement `check_expressions_agg`
[x] Implement the stitch logic for the aggregation case within the `check_executor` function.
[x] Any modifications of the schema must be reflected; what should we do in this case?
[x] A mysterious bug:

```rs
group_by_helper: groups Idx(GroupsIdx { sorted: false, first: [2, 0, 1, 3], all: [UnitVec: [2], UnitVec: [0], UnitVec: [1], UnitVec: [3]] })
in proxy_to_arg: gb GroupsIdx { sorted: false, first: [2, 0, 1, 3], all: [UnitVec: [1409383744], UnitVec: [1409377136], UnitVec: [1409299040], UnitVec: [1409383552]] }
```

[ ] Add an optional feature for `polars` so that we can do the comparsion.
[x] Reify expressions for aggregates.
  - [x] Seems we need to modify the design of the `Expr` struct; it cannot "own" the expression and it must be a weak reference?

[ ] The order of the column is wrong. So that the policy labels are tracked wrongly.
[ ] Add group size checking.
## Integration with DuckDB

[ ] Design and implement all the FFI APIs for C/C++ family.
