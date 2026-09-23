# syntax=docker/dockerfile:1
# Go version must match go.mod.
FROM golang:1.27.1-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w" -o /out/wallet ./cmd/wallet

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/wallet /wallet
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/wallet"]
CMD ["serve"]
