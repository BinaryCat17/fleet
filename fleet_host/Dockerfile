# syntax=docker/dockerfile:1

# ---------- stage 1: static musl build (builder is never shipped) ----------
FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev        # C toolchain for bundled SQLite
WORKDIR /app
COPY . .
# cache mounts => crates.io is hit only on the very first build
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release \
 && cp /app/target/release/fleet_host /out-fleet_host

# ---------- stage 2: THE IMAGE = one file ----------
FROM scratch
COPY --from=builder /out-fleet_host /fleet_host

# sqlite DB lives on the volume, NOT in the image
ENV DATABASE_PATH=/data/fleet.db
VOLUME ["/data"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/fleet_host", "--healthcheck"]

ENTRYPOINT ["/fleet_host"]