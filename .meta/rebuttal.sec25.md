We thank the reviewers for their constructive feedback, which will greatly improve our paper! Below, we address key concerns and outline revisions.

## Performance

The overhead primarily arises from enforcement at the *cell* granularity, requiring operations like projection and aggregation to perform linear-scans and label transformations. This increases latency depending on the relation’s tuple size. We plan to add thorough micro-benchmarks to identify specific bottlenecks.

Additionally, we will integrate database optimization techniques into our implementation, such as:

- foo
- bar

We will also include updated experimental results to demonstrate how these optimizations reduce overheads.

## Presentation

We acknowledge concerns about the clarity and completeness of the formal semantics. Planned revisions include:

- Focusing on key stepping rules with detailed explanations.
- Refactoring the formalism by defining missing terms (e.g., update) and correcting typos for consistency.
- Clarifying how the security monitor blocks bad queries and why stepping rules always progress.

## TEE Relevance

Picachv targets cloud-based data analytics scenarios, such as Data Clean Rooms (DCR), where users often distrust cloud service providers like Google Cloud. While Picachv enforces data use policies, risks of malicious tampering or data theft persist due to the untrusted environment.

TEEs address these concerns by:

- **Confidentiality:** Encrypting data-in-use, ensuring no leakage to the cloud.
- **Integrity:** Ensuring tamper-resistance for Picachv and user data.
- **Remote Attestation:** Allowing users to verify that the TEE is running genuine Picachv via its cryptographic measurement (hash), which can be compared against verified source code.

We will further clarify these points in the revision.

## Policy Language and Case Studies

We will enhance the discussion of the policy language by incorporating examples from diverse real-world applications, showcasing how our framework captures and enforces their requirements.

# Responses

## Review A

- **Performance:** Addressed above.
- **Policy composability:** Our rules require policies to be in descending order, as declassification to "higher levels" seems unreasonable in reality. Well-ordered policies can always be pattern-matched by our rules, allowing us to compare labels and "insert" appropriate requirements into the policy chain. We apologize for the unclear presentation in Fig. 5 and will revise it for clarity.
- **TEE Relevance:** Addressed above.

## Review B

- **TEE Connection:** Addressed above.
- **Dynamic policies:**
- **Access control:** While not our primary focus, this is feasible.

## Review C

`FUnary` does not block queries directly. The runtime monitor applies a sink function at the end of execution to check whether all data complies with policies (i.e., no labels remain). Permitted operations lower the label until requirements are met; non-compliant labels remain and trigger a block by the sink function. Label preservation offers flexibility. For example, if the `zipcode` column is redacted per policy but later requires summation, a transformation like `length(zipcode)` may be allowed.

## Review D

- Identifying protected parts of such data is a challenge for policy-makers and outside our scope. However, if predefined policies exist, Picachv can enforce them effectively.
- While we focus on relational algebra, our semantics are general and could be extended to other paradigms like ML algebra by identifying lattice elements, key operators, and expressions.
- Achieving full verification is engineering-intensive but feasible. As a research prototype, we prioritize verifying critical components to demonstrate our approach's viability.
- Policy interpretation is important but orthogonal to our work, which assumes predefined, machine-readable policies.

Despite these challenges, Picachv’s contributions establish a strong foundation for future extensions and broader applicability.