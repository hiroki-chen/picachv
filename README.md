# Picachv – Formally Verified Data Use Policy Enforcement for Secure Data Analytics

> [!CAUTION]
>
> Picachv is a research prototype and is still undergoing heavy construction and improvements. There might be broken links, packages, code that does not compile, etc. APIs may also be subject to change without notice in advance. Please do exercise your due diligence before using this prototype, especially for production environments.

> [!NOTE]
>
> Pikachu is the intellectual property and copyright of The Pokémon Company International, Inc. This work is intended for informational, educational, or personal use only and is not intended for commercial purposes. All rights reserved by the original copyright holders.

---

<img style="float:right" width=200 src="docs/pikachu.jpg">

The rapid advancement of computational devices and the rise of big data present unparalleled opportunities to accelerate scientific progress and drive innovation through data-driven decision-making. While these opportunities are ground-breaking, they are accompanied by a critical challenge: ensuring proper data *usage* in compliance with complex privacy policies. Examples of these complex policies include NIH *All-of-Us* project where besides the HIPAA law, NIH adds some customized privacy policies to restrict researchers' ability to use the patient data. Researchers must aggregate the statistics with no less than 20 people in a given aggregation group.

Yet, enforcing these policies poses a great challenge for us. First of all, manual checks in this scenario is generally infeasible due to the high error-rate of human audition in the face of the complexity of analytical tasks and the high cost of human effort. Relying on computers to automate policy enforcement seems a wise options; however, to the best of our knowledge, there is a lack of satisfatory solutions so far.

Why? This is because *data use policies*, unlike access controls that restrict who can see what, they instead ensure that *already authorized personal*  must comply with the proper data use operations. Addressing this problem remains non-trivial, especially when one tries to offer verifiaible security guarantees to the stake holders.

We advance the research in this area by introducing Picachv, a lightweight runtime security monitor that can be seamlessly integrated into existing query execution engines, plus verifiable formal guarantees written in Coq.

## Design and Workflow

Please see [Workflow](docs/workflow.md).

## Installation and Usage

Please see [Installation](docs/installation.md).

Picachv is written in native Rust and supports other languages (e.g., C/C++) by offering dynamic libraries and corresponding FFI API declarations.

## Citing the repo

This repository officially hosts the code for the paper accepted by USENIX Security Symposium 2025. To cite this work in your paper, please use the following bibtex format.

```tex
@inproceedings{chen2025picachv,
  title={Picachv: Formally Verified Data Use Policy Enforcement for Secure Data Analytics},
  author={Chen, Haobin Hiroki and Chen, Hongbo and Sun, Mingshen and Wang, Chenghong and Wang, XiaoFeng},
  booktitle={34th USENIX Security Symposium (USENIX Security 25)},
  year={2025},
  location={Seattle, WA, USA}
}
```

## Working with Trusted Execution Environments (TEEs)

Please see [TEEs](docs/TEEs.md).

## License

The code is subject to the Apache-2.0 license.
