FROM golang:1.25-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o wavicle .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o wavicle-cli ./cmd/wavicle-cli/

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=builder /build/wavicle /usr/local/bin/wavicle
COPY --from=builder /build/wavicle-cli /usr/local/bin/wavicle-cli

EXPOSE 6379 8080
ENTRYPOINT ["wavicle"]
