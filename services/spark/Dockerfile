FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

# Chuyển sang quyền root để cấu hình hệ thống
USER root

# Cài đặt curl để tải thư viện
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Khai báo phiên bản các JAR kết nối MinIO/S3
# Phiên bản 3.3.4 khớp hoàn chỉnh với nhân Hadoop đi kèm Spark 3.5.1
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

# Tải file JAR bỏ trực tiếp vào thư mục /opt/spark/jars/ của hệ thống
RUN curl -sS https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar

RUN curl -sS https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Trả lại quyền cho user mặc định của Apache Spark (UID 185) để container vận hành an toàn
USER 185