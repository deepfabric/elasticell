FROM deepfabric/elasticell-dev

RUN mkdir -p /apps/deepfabric

COPY ./ /go/src/github.com/deepfabric/elasticell

RUN cd /go/src/github.com/deepfabric/elasticell \
    && go test -v ./...

WORKDIR /apps/deepfabric