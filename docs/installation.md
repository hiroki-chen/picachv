# Installation
---

Before following the guide, please do note that Picachv is a standalone library that aims to be integrated with *existing data analytical frameworks*. This means that Picachv itself alone does nothing for you. Thus, "installing" Picachv is equivalent to saying that we integrate Picachv into your target analytical frameworks like Pandas, Polars, DuckDB, etc.

## Polars + Picachv

Luckily we have provided a modified version of Polars where we integrated Picachv. You can use this modified Polars framework as a start point to see how Picachv works.

## Docker (Recommended)

To facilitate build, we also provide a script for building your own docker environment for testing and playing around with Picachv. You can build the docker image by launching the following command:

```sh
$ docker build -f docker/Dockerfile -t picachv/picachv .
```

Then run the docker.

```sh
$ docker run -i -t picachv/picachv /bin/bash     
```

Then you should have all the required pacakges and tools installed for trying out Picachv and its accompanied Coq proofs.

## Verifying the Coq Proof

The Coq formalism and proofs are hosted in a separate repo, and we include this as a git submodule in `picachv-proof-lib`. In the docker container, just run

```sh
$ ./run --allow_admitted
```

to run the Coq compiler to type check the source code. If no error is thrown then all the proof checks are passed.
