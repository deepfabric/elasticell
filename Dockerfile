FROM deepfabric/elasticell-dev

RUN mkdir -p /apps/deepfabric

COPY ./ /go/src/github.com/deepfabric/elasticell

RUN cd /go/src/github.com/deepfabric/elasticell \
    && go test -v ./...

RUN cd /go/src/github.com/deepfabric/elasticell/cmd/pd \
    && go build -ldflags "-w -s" pd.go \
    && chmod +x ./pd \
    && mv ./pd /apps/deepfabric

RUN cd /go/src/github.com/deepfabric/elasticell/cmd/cell \
    && go build -ldflags "-w -s" cell.go \
    && chmod +x ./cell \
    && mv ./cell /apps/deepfabric

WORKDIR /apps/deepfabric