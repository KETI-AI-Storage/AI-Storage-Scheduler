FROM alpine:3.18

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY _output/bin/ai-storage-scheduler /usr/local/bin/ai-storage-scheduler

RUN chmod +x /usr/local/bin/ai-storage-scheduler

ENTRYPOINT ["ai-storage-scheduler"]
