# Build: docker build -f packaging/docker/Dockerfile -t infertap:local .
# Deployment, key permissions, and resource limits: docs/install.md#container.

FROM rust:1.97.1-slim AS build
# Compile vendored libzmq and its C++ bindings in the build stage.
RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --features zmq --locked

FROM debian:trixie-slim
# The runtime base supplies libc, libstdc++, and libgcc_s.
RUN useradd --system --uid 65532 --shell /usr/sbin/nologin infertap \
    # Fresh named volumes inherit ownership from this directory.
    && mkdir -p /out && chown 65532:65532 /out
COPY --from=build /src/target/release/infertap /usr/local/bin/infertap
COPY LICENSE NOTICE THIRD-PARTY-NOTICES /usr/share/doc/infertap/
USER 65532:65532
# The tap opens no listening ports.
ENTRYPOINT ["/usr/local/bin/infertap"]
CMD ["--help"]
