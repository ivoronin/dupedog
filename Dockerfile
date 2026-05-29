ARG GO_VERSION=1.25
FROM golang:${GO_VERSION}-alpine AS builder

ARG VERSION=dev
ARG COMMIT=none

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" -o /dupedog ./cmd/dupedog

FROM gcr.io/distroless/static-debian12:nonroot@sha256:d093aa3e30dbadd3efe1310db061a14da60299baff8450a17fe0ccc514a16639

LABEL org.opencontainers.image.source="https://github.com/ivoronin/dupedog"

COPY --from=builder /dupedog /usr/local/bin/dupedog
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/dupedog"]
