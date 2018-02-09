FROM golang:1.9.2

RUN mkdir -p /apps/deepfabric

COPY ./dist/cell  /apps/deepfabric
COPY ./entrypoint.sh /apps/deepfabric

RUN apt-get update \
    && apt-get -y install libsnappy-dev  \
    && apt-get -y install zlib1g-dev \
    && apt-get -y install libbz2-dev \
    && apt-get -y install libgtest-dev \
    && apt-get -y install libjemalloc-dev

RUN chmod +x /apps/deepfabric/cell \
    && chmod +x /apps/deepfabric/entrypoint.sh 

ENV ELASTICELL_HOME=/apps/deepfabric
ENV ELASTICELL_EXEC=cell

WORKDIR /apps/deepfabric

ENTRYPOINT ["/bin/sh", "./entrypoint.sh"]
