FROM ubuntu:16.04

RUN mkdir -p /apps/deepfabric

COPY ./dist/redis-proxy  /apps/deepfabric
COPY ./entrypoint.sh /apps/deepfabric

RUN chmod +x /apps/deepfabric/redis-proxy \
    && chmod +x /apps/deepfabric/entrypoint.sh 

ENV ELASTICELL_HOME=/apps/deepfabric
ENV ELASTICELL_EXEC=redis-proxy

WORKDIR /apps/deepfabric

ENTRYPOINT ["/bin/sh", "./entrypoint.sh"]
