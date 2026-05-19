FROM apache/superset:latest

USER root

COPY requirements.txt /tmp/requirements.txt
COPY init.sh /init.sh

RUN apt-get update && \
      apt-get install -y --no-install-recommends \
      python3-pip \
      python3-venv \
      netcat-openbsd \
      curl && \
      apt-get clean && \
      rm -rf /var/lib/apt/lists/*

RUN curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && \
      /app/.venv/bin/python /tmp/get-pip.py

RUN /app/.venv/bin/pip install --no-cache-dir \
      -r /tmp/requirements.txt

RUN sed -i 's/\r$//' /init.sh && \
      chmod +x /init.sh

USER superset

ENTRYPOINT ["/bin/bash", "/init.sh"]