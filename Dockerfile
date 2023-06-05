FROM rust:1-alpine3.17 as builder
RUN apk add --no-cache build-base bash curl
ADD . .
RUN cargo build --release

FROM rust:1-alpine3.17
COPY --from=builder target/release/ord /usr/local/bin/ord
