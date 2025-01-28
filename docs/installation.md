# Installation
---

Before following the guide, please do note that Picachv is a standalone library that aims to be integrated with *existing data analytical frameworks*. This means that Picachv itself alone does nothing for you. Thus, "installing" Picachv is equivalent to saying that we integrate Picachv into your target analytical frameworks like Pandas, Polars, DuckDB, etc.

## Polars + Picachv

Luckily we have provided a modified version of Polars where we integrated Picachv. You can use this modified Polars framework as a start point to see how Picachv works. Please see [Integration](integration.md).

## Preparations

### Docker (Recommended)

To facilitate build, we also provide a script for building your own docker environment for testing and playing around with Picachv. You can build the docker image by launching the following command:

```sh
$ docker build -f docker/Dockerfile -t picachv/picachv .
```

Then run the docker.

```sh
$ docker run -i -t picachv/picachv /bin/bash     
```

Then you should have all the required pacakges and tools installed for trying out Picachv and its accompanied Coq proofs.

### From Scratch

For people who want to buidl the whole artifact from scratch, it is required to install the following packages before proceeding, and we also assume that you are using a Ubuntu-like Linux system Our test OS is Ubuntu 24.04.1 LTS. Minor version does not matter.

1. Install system-wide dependencies:

```sh
$ sudo apt install -y build-essential clang python3 python3-pip python3-as-python libgmp-dev pkg-config clang protobuf-compiler
```

2. Install Rust toolchain and pin Rust version to nightly:

```sh
$ curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s
$ rustup -V
```

3. Install `opam` to get Coq.

```sh
$ bash -c "sh <(curl -fsSL https://opam.ocaml.org/install.sh)"
$ opam init
$ eval $(opam env)
$ opam pin add coq 8.19.0 # Pin Coq version to 8.19.0
```

4. Install `mold` as the linker for Picachv.

```sh
$ cd /tmp
$ git clone --branch stable https://github.com/rui314/mold.git
$ cd mold
$ ./install-build-deps.sh
$ cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ -B build
$ cmake --build build -j`nproc` 
$ sudo cmake --build build --target install
```

5. (Optional) Install python dependencies for benchmarks and data generation.

```sh
$ pip3 install polars coloredlogs pydantic pydantic_settings pyarrow
```

## Verifying the Coq Proof

The Coq formalism and proofs are hosted in a separate repo, and we include this as a git submodule in `picachv-proof-lib`. In the docker container, just run

```sh
$ ./run --allow_admitted
```

to run the Coq compiler to type check the source code. If no error is thrown then all the proof checks are passed.

## Building Picachv

It is quite simple to build Picachv. In the root directory, running

```sh
$ cargo build --release
```

is all you need. This command generates two dynamic libraries `libpicachv_core.so` and `libpicachv_api.so` in `target/release` and also a Rust library if you are targeting a Rust-based analytical framework like Polars. To know how to use the former C-style libraries, please kindly refer to [Integration](integration.md).
