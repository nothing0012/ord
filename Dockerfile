FROM rust:1-alpine3.17
ADD . .
RUN apk add --no-cache build-base bash
RUN cargo build --release --features "kafka"
