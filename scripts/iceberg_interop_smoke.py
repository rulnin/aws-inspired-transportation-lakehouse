"""Create a tiny Iceberg table in Spark for the Trino interoperability check."""

from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("lakehouse-infrastructure-smoke").getOrCreate()
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.infra_smoke")
        table = "lakehouse.infra_smoke.spark_trino_check"
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        (
            spark.createDataFrame(
                [("spark-to-trino-ok", 42)],
                ["payload", "expected_value"],
            )
            .writeTo(table)
            .using("iceberg")
            .tableProperty("write.format.default", "parquet")
            .create()
        )
        row = spark.table(table).first()
        if row is None or row.payload != "spark-to-trino-ok" or row.expected_value != 42:
            raise RuntimeError("Spark could not read back its Iceberg smoke-test row")
        print("Spark wrote and read the Iceberg smoke-test row")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
