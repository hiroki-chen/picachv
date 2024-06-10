# Benchmarking Picachv

In order to evaluate the policy checking overhead when Picachv is plugged into the existing data analytical frameworks, we utilize an industry-standardized benchmark called TPC-H to see how much time it takes for Picachv to fulfill each query.

Since the original implementation of TPC-H is not very tailored to our specific use case, we adapt it to our version and put all the relevant tools and files here. Some of the code is extracted from polar's port of TPC-H.

## Layout

- `dbgen`: The official implementation of the table generation code from TPC-H.
