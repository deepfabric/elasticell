FROM deepfabric/elasticell-dev

RUN mkdir -p /apps/deepfabric

RUN apt-get update \
    && apt-get -y install libsnappy-dev  \
    && apt-get -y install zlib1g-dev \
    && apt-get -y install libbz2-dev \
    && apt-get -y install libgtest-dev \
    && apt-get -y install libjemalloc-dev

COPY ./ /go/src/github.com/deepfabric/elasticell

RUN cd /go/src/github.com/deepfabric/elasticell/cmd/pd \
    && go build -ldflags "-w -s" pd.go \
    && chmod +x ./pd \
    && mv ./pd /apps/deepfabric

RUN cd /go/src/github.com/deepfabric/elasticell/cmd/cell \
    && go build -ldflags "-w -s" cell.go \
    && chmod +x ./cell \
    && mv ./cell /apps/deepfabric

WORKDIR /apps/deepfabric