FROM ubuntu:24.04

ENV OPERATOR=/usr/local/bin/ai-storage-scheduler \
    USER_UID=root \
    USER_NAME=discovery
COPY _output/bin/ai-storage-scheduler ${OPERATOR}

ENTRYPOINT ["/usr/local/bin/ai-storage-scheduler"]

USER ${USER_UID}