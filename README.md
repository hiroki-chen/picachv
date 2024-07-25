# Picachv – A Formally Verified Middleware For Your Data Analytical Frameworks!
Both academia and industry are leveraging cloud-aided large-scale data analytics to aid in decision-making, market prediction, and research, whose success hinges upon the massive amount of data.

This abundance of data, however, raises significant privacy concerns especially when tech giants, researchers, and organizations nowadays collect billions of sensitive user data, necessitating stringent and multi-dimensional protection measures. The need is further underscored by emerging regulatory requirements and increased user concerns. While there are effective mechanisms like encrypting the data using strong cryptographic algorithms and applying access controls to restrict *who can see what data*, i.e., data confidentiality, but few work controls *how data is used*, i.e., data use policy, or *purpose limitation* per GDPR that protects user privacy.

The status quo entails an efficient and secure approach to enforcing such data use policies to ensure a precise, automatic, and proactive protection.

We thus present Picachv, which is a mechanically verified (using Coq), automatic, portable security monitor for transparently enforcing data-use policies for the state-of-the-art data analytical systems.

## Build the Project

You must ensure you have installed the following dependencies on the computer:

- Rust toolchain (can be obtained via rustup)
- Protobuf (v23.0)

Then invoke cargo command in the root:

```sh
$ cargo build -r
```

## Integration with Other Systems

We currently provide two unofficial modification on the codebase of Polars and DuckDB to support the policy-checking functionalty. Other systems might be supported in the future. We also welcome community contributors to support more data analytical engines and systems on the market.

## The Proofs

The Coq formalization is hosted in another repo: https://github.com/hiroki-chen/pcd-proof Due to the upstream library restrctions, we can only use Coq 8.17.

