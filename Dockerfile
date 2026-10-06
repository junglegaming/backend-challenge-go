FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /wallet-service ./cmd/wallet-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /wallet-service /wallet-service

ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["/wallet-service"]
