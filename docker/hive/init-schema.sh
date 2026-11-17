#!/usr/bin/env bash
set -euo pipefail

export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx1G ${SERVICE_OPTS:-}"

if /opt/hive/bin/schematool -dbType "$DB_DRIVER" -info; then
  echo "Hive Metastore schema is already initialized"
else
  /opt/hive/bin/schematool -dbType "$DB_DRIVER" -initSchema
fi
