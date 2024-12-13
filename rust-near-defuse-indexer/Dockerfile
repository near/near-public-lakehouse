FROM rust:1.78.0 AS builder
WORKDIR /tmp/
COPY Cargo.toml Cargo.lock ./

COPY ./src ./src
RUN cargo build -p near-defuse-indexer --release

FROM ubuntu:22.04
RUN apt update && apt install -yy openssl ca-certificates && apt install libssl-dev
COPY --from=builder /tmp/target/release/near-defuse-indexer .