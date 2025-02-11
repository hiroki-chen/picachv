# Instructions for Artifact Evaluation for USENIX Security

Dear reviewers, thank you so much for your time and patience in evaluating our artifact. We will detail how to evaluate our artifact in this documentation. We hope this would streamline the process.

## Evaluation Environment Preparation

The first step is to have a working environment. Luckily we have provided you with a Dockerfile to setup the it quickly. You can refer to [Docker installation](./installation.md#docker-recommended) for information about building and running docker images.

## Reproducing results

This section introduces how the main claims and experimental results are reproduced.

### Verifying that Coq proofs are correct

This is relatively easy as we have already provided a simple Python script in `picachv-proof-lib`. Please run `./run --allow-admitted`. The Coq formalism layout can be found in `README.md` of that folder; we thus do not detail it here.

### Reproducing experimental results

There are several tables and figures in the paper that need to be reproduced, and we detail how as follows. Note that in each experiment, a `profile.log` file will be automatically generated in the folder where one runs the Rust program. This file contains the cost breakdown information which can be used as quick lookup of the runtime metrics.

- 