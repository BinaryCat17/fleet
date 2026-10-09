# syntax=docker/dockerfile:1

# ---------- stage 1 ----------
FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev ca-certificates
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release \
 && cp /app/target/release/fleet_provider_vast /out-provider

# ---------- stage 2: binary + CA bundle, nothing else ----------
FROM scratch
COPY --from=builder /out-provider /fleet_provider_vast
# needed because the provider calls the VAST API over HTTPS (reqwest/rustls)
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# API key: injected at runtime (env or /run/secrets), never baked into the image
ENV VAST_API_KEY_FILE=/run/secrets/vast_api_key
EXPOSE 50051

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/fleet_provider_vast", "--healthcheck"]

ENTRYPOINT ["/fleet_provider_vast"]
