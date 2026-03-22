FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget procps gnupg && \
    wget -q https://packages.adoptium.net/artifactory/api/gpg/key/public -O /tmp/adoptium.key && \
    install -D /tmp/adoptium.key /usr/share/keyrings/adoptium.asc && \
    echo "deb [signed-by=/usr/share/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb bookworm main" > /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && apt-get install -y --no-install-recommends temurin-17-jre && \
    rm -rf /var/lib/apt/lists/* /tmp/adoptium.key

ENV JAVA_HOME=/usr/lib/jvm/temurin-17-jre-amd64

COPY requirements.txt /opt/requirements.txt
RUN pip install --no-cache-dir -r /opt/requirements.txt

COPY analyze.py /opt/analyze.py
COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
