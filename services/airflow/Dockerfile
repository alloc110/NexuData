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

# ĐỔI THỨ TỰ: Cài đặt các package từ file requirements.txt TRƯỚC để sinh ra bộ nhân PySpark
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt"

# 4. Thiết lập phiên bản JAR và dùng Python tự dò thư mục để cắm "vũ khí" kết nối MinIO
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

RUN JAR_DIR=$(python3 -c "import pyspark, os; print(os.path.join(pyspark.__path__[0], 'jars'))") && \
      echo "Dò tìm thấy thư mục PySpark Jars tại: ${JAR_DIR}" && \
      curl -sS https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      -o "${JAR_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
      curl -sS https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      -o "${JAR_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"