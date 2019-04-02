FROM deepfabric/build as builder

COPY . /root/go/src/github.com/deepfabric/elasticell
WORKDIR /root/go/src/github.com/deepfabric/elasticell

RUN make proxy

FROM alpine:latest

COPY --from=builder /root/go/src/github.com/deepfabric/elasticell/dist/proxy /usr/local/bin/proxy

RUN mkdir -p /var/proxy/
RUN mkdir -p /var/lib/proxy/
RUN apk update
RUN apk add bind-tools wget netcat-openbsd

# Alpine Linux doesn't use pam, which means that there is no /etc/nsswitch.conf,
# but Golang relies on /etc/nsswitch.conf to check the order of DNS resolving
# (see https://github.com/golang/go/commit/9dee7771f561cf6aee081c0af6658cc81fac3918)
# To fix this we just create /etc/nsswitch.conf and add the following line:
RUN echo 'hosts: files mdns4_minimal [NOTFOUND=return] dns mdns4' >> /etc/nsswitch.conf

# Define default command.
ENTRYPOINT ["/usr/local/bin/proxy"]