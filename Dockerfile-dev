FROM golang

RUN apt-get update \
    && apt-get -y install libsnappy-dev  \
    && apt-get -y install zlib1g-dev \
    && apt-get -y install libbz2-dev \
    && apt-get -y install libgtest-dev \
    && apt-get -y install libjemalloc-dev

RUN go get github.com/deepfabric/c-nemo \
    && go get github.com/deepfabric/go-nemo
