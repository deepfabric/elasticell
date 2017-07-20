FROM deepfabric/elasticell

RUN mkdir -p /apps/deepfabric

COPY ./ /go/src/github.com/deepfabric/elasticell

RUN cd /go/src/github.com/deepfabric/elasticell/cmd/pd \
    && go build -ldflags "-w -s" pd.go \
    && mv ./pd /apps/deepfabric

RUN cd /go/src/github.com/deepfabric/elasticell/cmd/cell \
    && go build -ldflags "-w -s" cell.go \
    && mv ./cell /apps/deepfabric

WORKDIR /apps/deepfabric