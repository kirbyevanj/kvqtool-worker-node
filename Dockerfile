FROM golang:1.24 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
ENV GOTOOLCHAIN=local
ENV GONOSUMCHECK=github.com/kirbyevanj/*
ENV GOPRIVATE=github.com/kirbyevanj/*
ENV GOFLAGS=-mod=mod

COPY kvq-models/ /kvq-models/
COPY worker-node/ .

RUN go mod edit -replace github.com/kirbyevanj/kvqtool-kvq-models=/kvq-models \
    && go mod tidy \
    && CGO_ENABLED=0 go build -o /worker-node ./cmd/worker

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    ffmpeg \
    python3 \
    python3-pip \
    && pip3 install --break-system-packages onnxruntime numpy \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /worker-node /worker-node
ENTRYPOINT ["/worker-node"]
