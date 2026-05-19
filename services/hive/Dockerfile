FROM apache/hive:4.0.0

USER root

RUN apt-get update && apt-get install -y curl

# ===== Hadoop S3A connector (PHẢI ĐỂ Ở HADOOP tools lib) =====
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
      -o /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar

# ===== AWS SDK bundle =====
RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
      -o /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar

# ===== Postgres JDBC (Hive metastore) =====
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
      -o /opt/hive/lib/postgresql-42.7.3.jar

ENV HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/*