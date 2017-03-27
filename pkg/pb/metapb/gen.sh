#!/bin/bash
protoc --proto_path=$GOPATH/src/:. --gogo_out=plugins=grpc:.  ./metapb.proto