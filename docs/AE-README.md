# Instructions for Artifact Evaluation – USENIX Security

Dear reviewers,

Thank you for your time and patience in evaluating our artifact. This document provides a step-by-step guide to ensure a smooth evaluation process.

---

## 1. Evaluation Environment Setup

To begin, you need to set up a working environment. We provide a **Dockerfile** to facilitate a quick setup. Please refer to [Docker Installation Guide](./installation.md#docker-recommended) for instructions on building and running Docker images.

---

## 2. Tools Overview

Before proceeding with the evaluation, it is important to understand the key tools involved.

### 2.1 Data Generator

The data generator is a Python script located at `benchmark/prepare_data.py`. It is used to generate the dataset required for benchmarking.

#### Usage:
```sh
cd benchmark
python3 ./prepare_data.py --scale_factor=<factor> --skip-dbgen=<0/1> --percentage=<percentage>
```

#### Arguments:
- `scale_factor`: Defines the TPC database generation size (e.g., **1.0 ≈ 1 GiB of data**).
- `skip-dbgen`: **1** to skip database generation, **0** to enable it.
- `percentage`: Specifies how much of the generated database should be sampled (**e.g., 10 GiB * percentage_value**).

📌 **Note:**
- You can generate a **large database** (e.g., 10 GiB) in advance and use `percentage` to sample a smaller subset, reducing data generation time.
- If `percentage=1.0`, the full database will be used.

---

### 2.2 Policy Generator

The policy generator is a **Rust CLI tool** located at `tools/policy-generator`. **Run this tool only after generating the dataset using `prepare_data.py`**.

#### For Microbenchmarks (e.g., Cost Breakdown, etc.):
```sh
cd tools/policy-generator
cargo run -r -- --output-path ../../data/policies/micro --format parquet --is-micro
```

#### For the Full TPC-H Benchmark:
```sh
cargo run -r -- --output-path ../../data/policies/ --format parquet
```

📌 **Note:**
- Microbenchmarks require generating multiple policies. (TODO: Provide a streamlined way for reviewers to generate them easily.)

---

## 3. Reproducing Results

This section explains how to verify our **Coq proofs** and reproduce **experimental results** presented in the paper.

### 3.1 Verifying Coq Proofs

This process is straightforward. We provide a **Python script** in `picachv-proof-lib`.

#### To verify:
```sh
cd picachv-proof-lib
./run --allow-admitted
```
For more details on the Coq formalism, please refer to the **`README.md`** in the `picachv-proof-lib` directory.

---

### 3.2 Reproducing Experimental Results

The following steps detail how to reproduce the **tables and figures** from the paper.

📌 **Important Note:**
Each experiment automatically generates a `profile.log` file in the working directory. This file provides **cost breakdown information**, which can be used for quick runtime metric lookup.

---

#### 3.2.1 Macro-Benchmark Reproduction (Figure 15)

##### Step 1: Generate Data and Policies
```sh
# Generate data
cd benchmark
python3 ./prepare_data.py --scale_factor=1.0
```
```sh
# Generate policies
cd tools
cargo run -r -- --output-path ../../data/policies/ --format parquet
```

##### Step 2: Run Benchmarks
Navigate to `benchmark/polars-tpc` and execute the following for each query in Figure 15 (**Example: Q1**):

```sh
# With Picachv enabled:
cargo run -r -- --query=1 --policy-path ../../data/policies --enable-profiling  

# Without Picachv:
cargo run -r -- --query=1
```

##### Step 3: Extract and Visualize Results
- The execution should output:
  ```sh
  elapsed time: 123456789s
  ```
- To visualize **Figure 15**, use the Jupyter Notebook in `tools/plotting/macro.ipynb`. Simply replace the dataset with your newly obtained results.

---

#### 3.2.2 Micro-Benchmark Reproduction (Table 1, Figure 16)

##### Step 1: Generate Data and Policies
Follow the **[Data Generator](#data-generator)** instructions, then run:

```sh
cargo run -r -- --output-path ../../data/policies/micro --format parquet --is-micro --policy_type [A/B/C/D]
```

##### Step 2: Execute Benchmarking Script
```sh
cd benchmark/micro/polars
cargo run -r
```

##### Step 3: Extract Cost Breakdown
- The `profile.log` file contains the **runtime breakdown** for Table 1.
- To interpret Figure 16, use:
  - **Figure 16(a):** `non-agg: policy_eval` and `non-agg: process`
  - **Figure 16(b):** `aggregation: xxx`

---

## 4. Case Studies

Navigate to the `picachv-case-study` directory:
- `chronic` → Uses the **chronic dataset**.
- `healthcare` → Uses the **healthcare dataset**.

Inside `main.rs`, you will find tasks structured as follows:
```rust
/// Task 1
fn task1() {
    xxx.set_policy_checking(true) // <- note this
}

fn main() {
    println!("Task 1\n");
    task1();
    println!("Task 2\n");
    task2();
    println!("Task 3\n");
    task3();
}
```

- The order of **tasks corresponds to the paper's case study section**.
- `set_policy_checking(true)` acts as a **feature toggle** to enable policy checking.

#### To Reproduce Table 2:
Run the following command in each subfolder:
```sh
cargo run -r
```

---

## Final Notes
- If you encounter any **errors or inconsistencies**, please refer to the `README.md` files in the respective directories.
- Feel free to reach out with **questions or feedback**.

Thank you for your time and effort in evaluating our artifact!