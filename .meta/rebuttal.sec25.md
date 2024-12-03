We thank the reviewers for their constructive feedback, which will significantly improve our paper! Below, we address key concerns and outline planned revisions.

## Performance (A/B/C/D)

The performance issues stem from enforcing policies at the cell level, requiring operations like projection to perform linear scans. We will add detailed micro-benchmarks to identify and analyze bottlenecks.

We have already implemented the following optimizations:

- Parallelized policy-checking using the Rust library `rayon`, a well-established framework for parallel computation.
- Policy encoding with fast in-memory formats like Apache-Arrow to accelerate computation by exploiting memory locality.

Additionally, we plan to implement materialization, a well-established database technique that caches frequently-used and representative workloads. This approach is generic and directly applicable to Picachv’s policy-checking framework. Prior related research ([PrivateSQL](https://dl.acm.org/doi/pdf/10.14778/3342263.3342274), [IncShrink](https://arxiv.org/pdf/2203.05084)) and industry solutions ([SnowFlake](https://docs.snowflake.com/en/user-guide/views-materialized), [BigQuery](https://cloud.google.com/bigquery/docs/materialized-views-create)) show that this is feasible while simple.

Our revised paper will include benchmarks to demonstrate the effectiveness of these techniques and extend the discussion on hybrid static-dynamic schemes for future work.

## Presentation (A/B/C/D)

We acknowledge the reviewers’ concerns about clarity and completeness of the formalism. To improve presentation, we will:

- Revise the formalism’s structure, first introducing the complete syntax and then explaining the stepping rules for improved readability.
- Focus on key stepping rules, providing detailed explanations, and move full semantics to the appendix.
- Define missing terms (e.g., `update`) in both syntax and semantics.
- Elaborate on how the runtime monitor operates.

We will also polish the paper to fix language errors and make sentences more accessible.

## TEE Relevance (A/B/D)

TEEs are indispensable when data owners require *proofs* that data analytics comply with policies. TEEs provide cryptographic reports via remote attestation, ensuring that the runtime monitor and query is authentic and verified. While our mechanism does not rely exclusively on TEEs, it can also be applied to scenarios like on-premise databases.

## Policy composability (A)

Our rules enforce a descending order of policies, as declassification to higher sensitivity levels is impractical. Well-ordered policies can always be pattern-matched, allowing label comparisons and appropriate rule insertions into policy chains. We will revise Fig. 5 for clarity and apologize for its current presentation.

## Dynamic policies and Access controls (B)

Picachv supports dynamic policies since they are provided alongside the data, allowing flexibility across different use cases. Its policy composition capabilities also enable joint policies from multiple entities, supporting access controls within our framework.

## Blocking Bad Queries (C)

When an operation is applied to a cell carrying policies, as in the `FUnary` case, two outcomes are possible. If the operation is defined in the declassification operations for the current sensitivity level, the label is downgraded. Otherwise, the monitor preserves the label, preventing it from being downgraded. Eventually a sink function is applied at the end of query execution that checks that all data has only $\mathbf{L}$ labels remaining. If any non-$\mathbf{L}$ labels persist, the query result is blocked.

For example, if the `zipcode` column requires redaction and a query attempts to output it directly, the `zipcode` will retain a non-$\mathbf{L}$ label since the policy is unsatisfied. When the sink function is invoked, it detects the violation and blocks the output, ensuring the policy is upheld. `FUnary` alone doesn't block bad queries.

## Policy Expressiveness (C)

Our policies are designed around the five relational operators foundational to data analytics. Therefore, Picachv supports policies in the realm of relational algebra. We now already support HIPAA, CCPA, NIH All-of-Us policies widely used in reality. While our prototype’s policies are not comprehensive, policymakers can extend the language by adding new labels to the lattice as our high-level idea is generic.

## Limitations (D)

- Identifying protected parts of semi- or unstructured data is a challenge for policy-makers and outside our scope. However, if predefined policies exist, Picachv can enforce them effectively.
- While we focus on relational algebra, our semantics are general and could be extended to other paradigms like ML algebra; relational algebra is expressive enough to capture a wide range of important analytical tasks.
- Achieving full verification is engineering-intensive but feasible. As a research prototype, we prioritize verifying critical components.
- Policy interpretation is important but orthogonal to our work.

Despite these challenges, Picachv’s contributions establish a strong foundation for future extensions and broader applicability.