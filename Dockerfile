# syntax=docker/dockerfile:1.7
#
# DKV multi-stage build.
#
# WHY MULTI-STAGE:
#   The build stage carries the full Go toolchain (~500MB). We don't want
#   that in the runtime image. The final image is `distroless/static` which
#   has no shell, no package manager, no glibc — just the bare libraries
#   needed to run a static Go binary. ~5MB total.
#
# WHY DISTROLESS:
#   - Smaller attack surface (no shell means no `kubectl exec` to drop into,
#     which is good for prod).
#   - No CVEs from base packages we don't use.
#   - Forces CGO_ENABLED=0, which forces us to confront the dns/glibc
#     compatibility question early.
#
# ----- BUILD STAGE -----
FROM golang:1.25-alpine AS build

WORKDIR /src

# Cache module downloads as a separate layer. If go.{mod,sum} don't change,
# the dep download layer is reused — faster CI builds.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Build flags:
#   -trimpath: removes absolute paths from the binary (reproducible builds)
#   -ldflags="-s -w": strips debug info (~30% smaller binary)
#   CGO_ENABLED=0: pure Go, no libc dependency, runs on distroless/static
#   GOOS/GOARCH: targets linux/amd64 by default; override for arm64 builds
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=unknown

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
      -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/dkv \
      ./cmd/dkv

# ----- RUNTIME STAGE -----
# distroless/static is the smallest base for static Go binaries.
# `nonroot` variant runs as UID 65532 — required by most K8s PodSecurity policies.
FROM gcr.io/distroless/static-debian12:nonroot

# OCI image labels — useful for image registries and supply-chain tools.
LABEL org.opencontainers.image.title="dkv"
LABEL org.opencontainers.image.description="Distributed key-value store"
LABEL org.opencontainers.image.source="https://github.com/marcuskal/dkv"

COPY --from=build /out/dkv /usr/local/bin/dkv

# Default ports (overridden by ConfigMap in K8s):
#   9090 — gRPC client traffic
#   9091 — Raft consensus RPCs
#   9092 — Serf gossip
#   9093 — health/metrics HTTP
EXPOSE 9090 9091 9092 9093

# distroless/static:nonroot is already UID 65532; documenting for clarity.
USER 65532:65532

# Default command. K8s overrides this via the StatefulSet spec to point at
# the mounted ConfigMap path.
ENTRYPOINT ["/usr/local/bin/dkv"]
CMD ["--config", "/etc/dkv/dkv.yaml"]