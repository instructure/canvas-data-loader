FROM rust:1.23.0

WORKDIR /usr/src/canvas-data-loader
COPY . .

RUN apt-get update && apt-get -y --no-install-recommends install clang && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN cargo install && cargo clean && rm -rf ~/.cargo/registry ~/.cargo/git

ENV RUST_LOG info
CMD cdl-runner
