FROM rust:latest as cdl-build

RUN apt-get update && apt-get -y --no-install-recommends install clang && apt-get clean && rm -rf /var/lib/apt/lists/*

# Cache dependencies for faster builds
RUN cargo install cargo-build-deps
RUN cd /tmp && USER=root cargo new --bin canvas-data-loader
WORKDIR /tmp/canvas-data-loader
COPY Cargo.toml ./
RUN cargo-build-deps --release

# Copy in our source and build it
COPY src /tmp/canvas-data-loader/src
RUN cargo build --release

# Start a new build from a minimal image that we can copy the binary into
FROM debian:stretch-slim

RUN apt-get update && apt-get -y --no-install-recommends install libssl1.1 ca-certificates && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY --from=cdl-build /tmp/canvas-data-loader/target/release/cdl-runner .

ENV RUST_LOG info
CMD ./cdl-runner
