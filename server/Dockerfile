# syntax=docker/dockerfile:1.7
#
# Build from the repository root (required — server/go.mod has a
# `replace ../pkg` directive, so the pkg tree must be in the build context):
#
#   docker build -f server/Dockerfile -t locator .
#
# Run (mount your own config and a data volume):
#
#   docker run --rm \
#     -v $PWD/server/config.ex.yml:/etc/locator/config.yml:ro \
#     -v locator-data:/var/lib/locator \
#     -p 8881:8881 -p 9991:9991 locator

FROM golang:1.26-alpine AS build

WORKDIR /locator

COPY pkg/ ./pkg/
COPY server/go.mod server/go.sum ./server/

WORKDIR /locator/server

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY server/ .

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags='-s -w' -o /out/locator ./cmd

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/locator /usr/local/bin/locator

# 8881 = registry gRPC (client-facing), 9991 = raft RPC (cluster-internal).
# Adjust via config if you change ports.
EXPOSE 8881 9991

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/locator"]
CMD ["-config", "/etc/locator/config.yml"]