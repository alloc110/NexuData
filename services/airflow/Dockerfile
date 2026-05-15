FROM apache/airflow:2.9.1

USER root

# Cài đặt các thư viện hệ thống cần thiết cho confluent-kafka và ClickHouse
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      librdkafka-dev \
      python3-dev \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*

USER airflow

# Cài đặt các thư viện Python
RUN pip install --no-cache-dir \
      confluent-kafka \
      python-dateutil \
      faker \
      airflow-clickhouse-plugin