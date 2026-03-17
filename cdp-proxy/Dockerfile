FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY . .
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /cdp-proxy .

FROM alpine:3.20
COPY --from=builder /cdp-proxy /usr/local/bin/cdp-proxy
ENTRYPOINT ["cdp-proxy"]
