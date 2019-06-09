#!/bin/bash

PORT=${INFLUXDB_BALANCER_PORT:-8888}

# create kafka cluster
curl -i -XPOST http://127.0.0.1:${PORT}/kafka_config/clusters -d '{"name":"kafka-default","zookeeper":["127.0.0.1:2181"],"bootstrap-server":["127.0.0.1:9092"]}'

# create kafka output
curl -i -XPOST http://127.0.0.1:${PORT}/kafka_config/outputs -d '{"name":"influxdb-default","topic":"influxdb-default","cluster":"kafka-default"}'

# create kafka databases database 2 kafka output
curl -i -XPOST http://127.0.0.1:${PORT}/kafka_config/databases -d '{"name":"test","output":["influxdb-default"]}'

# create influxdb output
curl -i -XPOST http://127.0.0.1:${PORT}/config/outputs -d '{"buffer-size-mb":128,"location":"http://127.0.0.1:8086","max-batch-kb":200,"max-delay-interval":"1s","name":"influxdb-default","rocksdb-buffer-size-mb":10240,"skip-tls-verification":false,"timeout":""}'

# create databases
curl -i -XPOST http://127.0.0.1:${PORT}/config/databases -d '{"name":"test","output":["influxdb-default"]}'

# influxdb create database
curl -i -XPOST "http://127.0.0.1:${PORT}/query?q=CREATE+DATABASE+%22test%22+WITH+DURATION+2w+REPLICATION+1+NAME+%22default%22"
