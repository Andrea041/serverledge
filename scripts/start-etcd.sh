#!/bin/sh

docker run -d --name etcd-server \
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
