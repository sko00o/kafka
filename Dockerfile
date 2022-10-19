FROM golang:1.17 AS builder

ARG APP
ARG TAG
WORKDIR /work
COPY . .
RUN mkdir -p bin
RUN go build -o bin/kafka cmd/kafka-cli/main.go

FROM debian:bullseye-slim
ENV TZ=Asia/Shanghai
RUN set -x && apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /work/bin/kafka .
ENTRYPOINT ["/app/kafka"]
CMD []
