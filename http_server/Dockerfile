FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Kyiv

RUN apt-get update && apt-get install -y \
    python3 \
    python3-venv \
    python3-pip \
    ca-certificates \
    tzdata \
    && ln -fs /usr/share/zoneinfo/$TZ /etc/localtime \
    && dpkg-reconfigure --frontend noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /eva_productivity

COPY . .

RUN python3 -m venv /eva_productivity/.venv && \
    /eva_productivity/.venv/bin/python -m pip install --no-cache-dir influxdb3-python requests

CMD ["/eva_productivity/.venv/bin/python", "/eva_productivity/main.py"]