import os
import tempfile

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def main():
    builder = (
        SparkSession.builder
        .appName("KPI-Assessment-Smoke-Test")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "2")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    try:
        print("Spark version:", spark.version)

        data = [
            (1, "completed"),
            (2, "pending"),
            (3, "failed")
        ]

        df = spark.createDataFrame(
            data,
            ["transaction_id", "status"]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            delta_path = os.path.join(temp_dir, "transactions")

            # Write to Delta Lake
            df.write.format("delta").mode("overwrite").save(delta_path)

            # Read from Delta Lake
            result = (
                spark.read
                .format("delta")
                .load(delta_path)
            )

            result.show()

            assert result.count() == 3

            print("Delta Lake write/read successful!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()