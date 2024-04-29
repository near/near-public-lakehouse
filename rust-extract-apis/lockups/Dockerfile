FROM rust:1.75.0 AS builder
WORKDIR /tmp/
COPY Cargo.toml Cargo.lock ./

COPY . .
RUN cargo build -p lockups --release

FROM ubuntu:22.04
RUN apt update && apt install -yy openssl ca-certificates && apt install libssl-dev
COPY --from=builder /tmp/target/release/lockups .
ENTRYPOINT [ "./lockups" ]
