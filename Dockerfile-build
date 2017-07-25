FROM deepfabric/elasticell-dev

RUN mkdir -p /apps/deepfabric

COPY ./entrypoint-build.sh /apps/deepfabric

RUN apt-get update \
    && apt-get install -y git \
    && chmod +x /apps/deepfabric/entrypoint-build.sh

WORKDIR /apps/deepfabric
ENTRYPOINT ./entrypoint-build.sh