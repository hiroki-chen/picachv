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

[x] Add an optional feature for `polars` so that we can do the comparsion.
[x] Reify expressions for aggregates.
  - [x] Seems we need to modify the design of the `Expr` struct; it cannot "own" the expression and it must be a weak reference?
[x] Implement join: how to "transform"?
[x] The order of the column is wrong. So that the policy labels are tracked wrongly.
[x] Add group size checking.

[ ] Support more binary expressions.
[x] Support `Order By` which re-arranges the dataframe.
## Integration with DuckDB

[x] Design and implement all the FFI APIs for C/C++ family.


## Optimizations
[x] Policy compression
[x] Faster de- and serialization.
[x] Parallelism
## Misc
[ ] Add a licence?
[x] There is a conflict between polars' thread pool and picachv's thread pool.

Two interfaces causing major overhead:

[x] `reify_expression`:
  - `get_ctx_mut` that locks the global context which seems unnecessary.
  - `expr_arena` lock and unlock stuff (writer waiting other writer but writers always modify different ).

[x] `convert_record_batch` introduces extra overhead which seems unnecessary.
  - Or, we simply don't convert but rather maintains a `Arc<RecordBatch>` to it.

[x] Our performance is even better than Polars. This is weird.
    This is due to the calculation error.
    ```
    [     Task A     ]
        [       Task B    ]
    ```

    In the above case if we want to substract the time used for fulfillign task b from the total time then we might get the wrong information; however, we don't want to
    include the time used for Task B.

# USENIX Security Artifact Evaluation

Prepare for the documentation for reproducing the results in the paper.