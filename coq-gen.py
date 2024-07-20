#!/usr/bin/env python3
# Generates the coq simulation of the THIR of the source code

import logging
import coloredlogs
import subprocess

coloredlogs.install()


def main():
    logging.info("Translating all files to Coq")

    # Check if coq-of-rust is installed under the PATH
    if not subprocess.run(["which", "cargo-coq-of-rust"]).returncode == 0:
        logging.error("coq-of-rust is not installed; please install it")
        exit(1)

    ret = subprocess.run(
        [
            "cargo",
            "+nightly-2024-01-11",
            "coq-of-rust",
            "--",
            "-p",
            "picachv-core",
            "--features",
            "coq",
        ],
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )

    if ret.returncode != 0:
        logging.error("Failed to translate files to Coq")
        logging.error(ret.stderr.decode("utf-8"))
        exit(1)

    logging.info("Successfully translated all files")
    logging.info("Copying the generated files to the Coq project")

    ret = subprocess.Popen(
        'find ./picachv-core/src  -maxdepth 10 -type f -name "*.v" -exec mv "{}" ./picachv-proof \\;',
        shell=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )


if __name__ == "__main__":
    main()
