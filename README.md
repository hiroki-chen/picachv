# Picachv – A Formally Verified Middleware For Your Data Analytical Frameworks!
Both academia and industry are leveraging cloud-aided large-scale data analytics to aid in decision-making, market prediction, and research, whose success hinges upon the massive amount of data.

This abundance of data, however, raises significant privacy concerns especially when tech giants, researchers, and organizations nowadays collect billions of sensitive user data, necessitating stringent and multi-dimensional protection measures. The need is further underscored by emerging regulatory requirements and increased user concerns. While there are effective mechanisms like encrypting the data using strong cryptographic algorithms and applying access controls to restrict *who can see what data*, i.e., data confidentiality, but few work controls *how data is used*, i.e., data use policy, or *purpose limitation* per GDPR that protects user privacy.

The status quo entails an efficient and secure approach to enforcing such data use policies to ensure a precise, automatic, and proactive protection.

We thus present Picachv, which is a mechanically verified (using Coq), automatic, portable security monitor for transparently enforcing data-use policies for the state-of-the-art data analytical systems.

## Build the Project

You must ensure you have installed the following dependencies on the computer:

- Rust toolchain (can be obtained via rustup)
- Protobuf (v3.21.0; must be built from *source*)
- libarrow-dev (v53)
- cmake
- clang (we are using mold)
- mold
- libre2-dev
- xsimd
- Apache Arrow (Must be built from *source*)

Please be rather careful about the protobuf versioning especially if you have Anaconda or equivalent installed on your system because this would cause confusion when cmake tries to identify the correct protobuf compiler, library, and header files. It is highly recommended that Anaconda3 is deactivated before building the project. Also, please install protobuf from source and never from any pre-built binary distribution (e.g., via `apt`). These binary distributions are either too old, or simply break the compilation.

Then invoke cargo command in the root:

```sh
$ cargo build -r
```

## Integration with Other Systems

We currently provide two unofficial modification on the codebase of Polars and DuckDB to support the policy-checking functionality. Other systems might be supported in the future. We also welcome community contributors to support more data analytical engines and systems on the market.

- Polars.
- DuckDB.

## The Proofs

The Coq formalization is hosted in another repo: https://github.com/hiroki-chen/pcd-proof Due to the upstream library restrctions, we can only use Coq 8.17.

