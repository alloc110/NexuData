FROM apache/airflow:2.9.1

USER root
# 1. Cài đặt các thư viện hệ thống cần thiết cho confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      librdkafka-dev \
      pkg-config \
      python3-dev \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*

# 2. Chuyển về user airflow
USER airflow
COPY requirements.txt /tmp/requirements.txt

# 3. Cài đặt python packages với file constraint cố định của bản 2.9.1-python3.11
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt"