FROM apache/airflow:2.9.1

USER root

# 1. Cài đặt các thư viện hệ thống cho Kafka VÀ thêm OpenJDK 17 + procps cho Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      librdkafka-dev \
      pkg-config \
      python3-dev \
      openjdk-17-jre-headless \
      procps \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*

# 2. Thiết lập biến môi trường hệ thống cho Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}

# 3. Chuyển về user airflow để cài đặt Python Packages
USER airflow
COPY requirements.txt /tmp/requirements.txt

# 4. Cài đặt các package từ file requirements.txt của bạn kèm constraints chuẩn
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt"