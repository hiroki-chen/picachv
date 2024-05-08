# High-Level Workflow of `Picachv`

In this doc we briefly introduce the working mechanism of `Picachv`. We split the process into the following steps.

## Context Initialization

For each data analytical session we will have an active context where all the relevant policies and data are stored. Besides we have a global monitor that remains active as long as there exists at least one active context. Interested readers are referred to the `picachv-monitor` crate for detailed information.

Basically, for a task to be checked, one needs to do the following.

```rust
// Initialize a global instance of security monitor.
picachv_api::init_monitor();
// Initialize a new context.
let ctx_id = open_new().expect("fail to initialize new context");
```

## Data and Policy Registration

Next the for the dataframe being analyzed, its guardian policy must be registered into the context.

```rust
let policy = some_policy!();
let uuid = register_policy_dataframe(ctx_id, policy).expect("fail!");
```

Afterwards, depending on the data analytical framework, this context ID must be stored somewhere. For example, we can directly store it into `df`.

```rust
df.set_uuid(uuid);
```

## Creates the Analytical Query

Now everthing is set up, and we can issue queries.

```rust
let out = df
    .lazy()
    .select([col("a"), col("b")])
    .filter(lit(1).lt(col("a")))
    .group_by([col("b")])
    .agg(vec![col("a").sum()])
    .set_ctx_id(ctx_id)
    .collect()?;

// Error: invalid operation: Possible policy breach detected; abort early.
// Because we do not apply the policy on the column "b".
```
