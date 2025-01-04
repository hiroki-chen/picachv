FROM ubuntu:24.04

RUN apt update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y \
  build-essential \
  libtool \
  autoconf \
  curl \
  git \
  wget \
  python3 \
  python3-pip \
  libgmp-dev \
  pkg-config \
  opam \
  clang

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin/:${PATH}"

RUN opam init --disable-sandboxing
RUN eval $(opam env)
RUN opam install --yes coq

ENV PATH="/root/.opam/default/bin:${PATH}"

WORKDIR /picachv

COPY .. /picachv

RUN rustup -V
RUN git clone https://github.com/hiroki-chen/polars.git /polars
RUN git clone --branch stable https://github.com/rui314/mold.git /mold
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y protobuf-compiler
WORKDIR /mold
RUN ./install-build-deps.sh
RUN cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ -B build
RUN cmake --build build -j$(nproc)
RUN cmake --build build --target install

WORKDIR /picachv

RUN pip3 install --break-system-packages \
  polars \
  coloredlogs \
  pydantic \
  pydantic_settings \
  pyarrow
