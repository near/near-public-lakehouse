FROM rust:1.81 AS builder
WORKDIR /tmp/

# Optimize caching
COPY Cargo.toml Cargo.lock ./
RUN cargo fetch 

COPY ./src ./src
RUN cargo build -p near-defuse-indexer --release

FROM ubuntu:22.04
RUN apt update && apt install -yy --no-install-recommends openssl ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/target/release/near-defuse-indexer .
RUN chmod +x near-defuse-indexer

ENTRYPOINT ["./near-defuse-indexer"]
