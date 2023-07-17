FROM amazoncorretto:11
MAINTAINER jmeredith@vantiq.com

RUN mkdir -p /opt/vantiq/bin
RUN mkdir -p /opt/vantiq/config

# COPY logback.xml /opt/vantiq/config
COPY serviceConnector.sh /opt/vantiq/bin

EXPOSE 8888

ENTRYPOINT ["/opt/vantiq/bin/serviceConnector.sh"]
CMD []