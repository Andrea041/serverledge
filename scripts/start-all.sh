#!/bin/sh

# start etcd
docker run -d --rm --name etcd-server \
  -p 2379:2379 -p 2380:2380 \
  gcr.io/etcd-development/etcd:v3.5.14 \
  /usr/local/bin/etcd \
  --name etcd-server \
  --data-dir=/etcd-data \
  --advertise-client-urls http://127.0.0.1:2379 \
  --listen-client-urls http://0.0.0.0:2379 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --initial-cluster etcd-server=http://127.0.0.1:2380 \
  --initial-cluster-token etcd-cluster-1 \
  --initial-cluster-state new


# start influxdb
docker run -d --rm -p 8086:8086 --name InfluxDb     -e DOCKER_INFLUXDB_INIT_MODE=setup \
            -e DOCKER_INFLUXDB_INIT_USERNAME=user \
            -e DOCKER_INFLUXDB_INIT_PASSWORD=password \
            -e DOCKER_INFLUXDB_INIT_ORG=serverledge \
            -e DOCKER_INFLUXDB_INIT_BUCKET=dqn \
      -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=serverledge \
      influxdb