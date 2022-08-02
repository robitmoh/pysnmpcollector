FROM python:3.7-alpine as base
LABEL maintainer="robitmoh"
LABEL description="pysnmpcollector can gather snmpdata from multiple hosts at once and write data to InfluxDB"

FROM base as builder

RUN mkdir /install
WORKDIR /install

COPY requirements.txt /requirements.txt

RUN apk add gcc py3-psutil net-snmp-tools net-snmp-dev musl-dev linux-headers

RUN pip install --prefix /install --no-cache-dir -r /requirements.txt

#RUN apk del gcc net-snmp-dev musl-dev linux-headers  # a builder miatt nem kell?

FROM base

COPY --from=builder /install /usr/local
COPY ./ /pysnmpcollector/
RUN rm -Rf cfg/*
copy docker/docker_start.sh /pysnmpcollector/docker_start.sh
RUN  chmod 755 /pysnmpcollector/docker_start.sh
WORKDIR /pysnmpcollector

RUN apk add --no-cache tzdata gettext net-snmp-agent-libs
CMD ./docker_start.sh
