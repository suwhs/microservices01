#!/bin/sh

cd commands/service1/
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -tags netgo -ldflags '-w'
minikube image build -t msvc/service-1

cd ../../
cd commands/service2
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -tags netgo -ldflags '-w'
minikube image build -t msvc/service-2

