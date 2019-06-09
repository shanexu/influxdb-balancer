.DEFAULT_GOAL := build-all

GIT_REV=$(shell git rev-parse --short HEAD)

build-all: influxdb-balancer

balancer-deps:
	@mkdir -p bin config

influxdb-balancer: balancer-deps
	go build -mod=vendor -ldflags '-X github.com/shanexu/influxdb-balancer/relay.GitRevision=${GIT_REV}' -i -o bin/influxdb-balancer cmd/influxdb-balancer/main.go

static: balancer-deps
	go build -mod=vendor -ldflags '-extldflags "-static -llz4 -lzstd" -X github.com/shanexu/influxdb-balancer/relay.GitRevision=${GIT_REV}' -i -o bin/influxdb-balancer cmd/influxdb-balancer/main.go

builder:
	docker build -t influxdb-balancer-builder -f Dockerfile_build_centos7 .

static-with-docker: builder
	docker run -it -v $(shell pwd):/root/influxdb-balancer --rm influxdb-balancer-builder /bin/bash -c 'unset GOPATH && cd /root/influxdb-balancer && make clean && make static'

clean:
	@rm -rf bin
