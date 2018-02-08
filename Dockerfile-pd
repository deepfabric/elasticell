FROM ubuntu:16.04

RUN mkdir -p /apps/deepfabric

COPY ./dist/pd /apps/deepfabric
COPY ./entrypoint.sh /apps/deepfabric

RUN chmod +x /apps/deepfabric/pd \
    && chmod +x /apps/deepfabric/entrypoint.sh 

ENV ELASTICELL_HOME=/apps/deepfabric
ENV ELASTICELL_EXEC=pd

WORKDIR /apps/deepfabric

ENTRYPOINT ["/bin/sh", "./entrypoint.sh"]
