# Core Logic of the `Picachv` Security Monitor

All the code is hosted in the `picachv-core` crate.

## Policies

Policies are essentially a chain of cell-level labels defined in the `picachv-core/policy` module. The formal model can be found in Coq proofs.

## Relational Operators
Since `Picachv` works as a security monitor that acts like a "middleware" for modern data analytical frameworks (e.g., Polars, Apache SparkQL, DuckDB, etc.), it tries to mimic the high-level relational operators described by these frameworks. These include:

- Select ($\sigma_{\varphi}(R)$): This operator does the filtering jobs.
- Projection ($\pi_{\ell}(R)$): This operator projects on a given dataframe $R$ and produces the results according to $\ell$ which might contain some expressions.
- Aggregate ($\Gamma_{gb, \ell}(R)$): This operator aggregates the given groups according to `gb` and `ell` (e.g., `SUM`).
- Union ($R_1 \cup R_2$): This operator just unions the given dataframes given they share the same schema.
- Join ($R_1 \bowtie R_2$): This operator joins the given dataframes by a predefined rule (e.g., natural, etc.).

We refer to these operators as "plans" defined in the `plan` module which can be found at `picachv-core/plan/` directory.

## Expressions

Relational operators sometimes contain which is referred to as "expressions". These expressions are heavily used to perform certain tasks. For example, consider the following plan:

$
  \pi_{A + DATE(B)}(R)
$

Expressions are defined in `picachv-core/expr`.

## Arenas

Since plans are not constructed by `Picachv`, for recursively defined data structures it is relatively hard to let us manage the resources on heap since Rust complains about the ownership. We instead choose the `Arena` solution where objects are allocated on an area managed by "unique id"s that minic the pointer-based solutions.

For example,

```rust
BinaryExpr {
    left: Uuid,
    op: binary_operator::Operator,
    right: Uuid,
    values: Option<Vec<Vec<AnyValue>>>,
},
```

Thus, building expressions and plans from the caller side becomes rather simple: it tells `Picachv` the arguments of plans or expressions.

## Message Exchange

So, how does `Picachv` know how to construct expressions and plans? We do this via Google ProtoBuf. All the structs are defined in `picachv-message/proto`, and the Rust-gernated files are located in `src`.

## APIs

We expose high-level APIs to the callers by the `picachv-api` crate which currently supports:

- Rust native API: `native.rs`. (For Rust one can also import the `picachv` single crate that exports all symbols.)
- C-APIs: `capi.rs`
- Python-APIs: `pyapi.rs`
- Java-APIs: `jvapi.rs`

Common APIs and their functionalities:

- `build_expr`: Tell `Picachv` to build an expression in the expression arena.
- `register_policy_dataframe`: registers a dataframe guarded by a policy.
- `execute_epilogue`: Checks if an executor (i.e., operator) is allowed to execute; it also applies the dataframe transform, if any.
- `finalize`: Checks if the result can be output.
- `reify_expression`: To check certain expressions we need to know the values they carry. In this case the caller is responsible for "reifying" expressions (like function application or aggregate expressions).


  ![fig](./Untitled%20Diagram.svg)

## Checking Plans and Expressions

Please refer to `picachv-core/plan/mod.rs` as well as `picachv-core/expr/mod.rs` for more information. We suggest giving a rough look at the Coq semantics before doing code reviews.
