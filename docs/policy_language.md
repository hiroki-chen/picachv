# Defining the Policy Through Policy Definition Language (PDL)

Writing policies can be a great burden and poses limitation to those (e.g., hospital staffs, users, etc.) who are unfamiliar with programming languages. To bridge this gap, we also design a simple domain specific language for describing the policy that guards the desired dataset, which aims to be human-readable, simple, and expressive.

## Features of the policy
- Filtering requirements: These enforce which piece of data can be used for analysis.
- Transform requirements: These enforce how a data cell should be "transform"ed, i.e., which expressions or functions should be applied to it. For example, the policy may read "ages must be replaced with "*" or generalized to a group of 30 people".
- Aggregate requirements: These enforce how a data should be aggregated to reduce the risk of identification of a single individual (see, e.g., HIPAA). For example, the policy may read "only aggregated statistics of the regional hospital discharge can be released, with group size no smaller than 20".
- Noise requirements: These enforce, if any, how much (DP) noise should be added to the output of the aggregated statistics.
- Cell-level granularity: The policy maker can selectively specify different policies for each cell of a single record in the dataset.
- Joinable policies: It is a common practice that datasets are merged to perform a certain analytical task. However, this means that policies might also be merged. Our policy natively supports such operations.

## The Syntax of the Language

Under construction.

## Implementation

We implement a parser atop the `lalrpop` crate. @ya0guang, can you please do it?
