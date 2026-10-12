# syntax=docker/dockerfile:1

# ---------- stage 1: static musl build (builder is never shipped) ----------
FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev        # C toolchain for bundled SQLite
WORKDIR /app
COPY proto ./proto
COPY sdk/rust ./sdk/rust
COPY fleet-host ./fleet-host
# cache mounts => crates.io is hit only on the very first build
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/fleet-host/target \
    cargo build --release --manifest-path fleet-host/Cargo.toml \
 && cp /app/fleet-host/target/release/fleet-host /out-fleet-host

# ---------- stage 2: THE IMAGE = one file ----------
FROM scratch
COPY --from=builder /out-fleet-host /fleet-host

# sqlite DB lives on the volume, NOT in the image
ENV DATABASE_PATH=/data/fleet.db
VOLUME ["/data"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/fleet-host", "--healthcheck"]

ENTRYPOINT ["/fleet-host"]