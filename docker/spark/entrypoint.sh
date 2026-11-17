#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "Usage: entrypoint.sh <Spark application> [arguments...]" >&2
  exit 2
fi

exec /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.jars.ivySettings=/opt/lakehouse/ivysettings.xml \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.lakehouse.warehouse=s3://lakehouse/warehouse \
  --conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.lakehouse.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.lakehouse.s3.path-style-access=true \
  --conf spark.sql.catalog.lakehouse.s3.access-key-id="${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}" \
  --conf spark.sql.catalog.lakehouse.s3.secret-access-key="${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}" \
  --conf spark.sql.catalog.lakehouse.client.region=us-east-1 \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key="${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}" \
  --conf spark.hadoop.fs.s3a.secret.key="${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.driver.memory=2g \
  "$@"
