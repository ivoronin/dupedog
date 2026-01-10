ARG GO_VERSION=1.25
FROM golang:${GO_VERSION}-alpine AS builder

ARG VERSION=dev
ARG COMMIT=none

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" -o /dupedog ./cmd/dupedog

FROM alpine:3.21
COPY --from=builder /dupedog /usr/local/bin/dupedog
ENTRYPOINT ["dupedog"]
