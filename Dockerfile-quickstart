FROM golang:1.9.2

RUN mkdir -p /apps/deepfabric/cfg \
    && mkdir -p /apps/deepfabric/log \
    && mkdir -p /apps/deepfabric/pd1/data \
    && mkdir -p /apps/deepfabric/pd2/data \
    && mkdir -p /apps/deepfabric/pd3/data \
    && mkdir -p /apps/deepfabric/cell1/data \
    && mkdir -p /apps/deepfabric/cell2/data \
    && mkdir -p /apps/deepfabric/cell3/data 

COPY ./quickstart-cfgs /apps/deepfabric/cfg
COPY ./dist/redis-proxy  /apps/deepfabric
COPY ./dist/pd  /apps/deepfabric
COPY ./dist/cell  /apps/deepfabric
COPY ./entrypoint-quickstart.sh /apps/deepfabric

RUN apt-get update \
    && apt-get -y install libsnappy-dev  \
    && apt-get -y install zlib1g-dev \
    && apt-get -y install libbz2-dev \
    && apt-get -y install libgtest-dev \
    && apt-get -y install libjemalloc-dev

RUN chmod +x /apps/deepfabric/pd \
    && chmod +x /apps/deepfabric/cell \
    && chmod +x /apps/deepfabric/redis-proxy \
    && chmod +x /apps/deepfabric/entrypoint-quickstart.sh

WORKDIR /apps/deepfabric

ENTRYPOINT ./entrypoint-quickstart.sh
