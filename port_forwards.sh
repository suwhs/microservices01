#!/bin/sh
kubectl port-forward --namespace default svc/redis-master 6379:6379 &
kubectl port-forward --namespace default svc/kafka-0 9092:9092 &
kubectl port-forward --namespace default svc/rabbitmq 5672:5672 &
