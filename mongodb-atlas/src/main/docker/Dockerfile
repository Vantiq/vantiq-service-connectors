FROM amazoncorretto:11
MAINTAINER jmeredith@vantiq.com

# the service connector service adds config and secrets relative to /opt/vantiq. The docker build must create this
# directory
RUN mkdir -p /opt/vantiq

# use it as the root of the service connector deployment
ADD mongodb-atlas.tar /opt/vantiq

WORKDIR /opt/vantiq

EXPOSE 8888

ENTRYPOINT ["./mongodb-atlas/bin/mongodb-atlas"]
CMD []