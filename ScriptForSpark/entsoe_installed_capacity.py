import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


spark = (
    SparkSession.builder
    .appName("ENTSOE_Installed_Capacity_GCS")
    .getOrCreate()
)


def _build_period_key(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d")
    end = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d")
    return f"{start}-{end}"


def _path_exists(path):
    hconf = spark._jsc.hadoopConfiguration()
    j_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = j_path.getFileSystem(hconf)
    return fs.exists(j_path)


def _append_without_existing_ids(df, output_path, partition_cols):
    """Append new records while skipping IDs already present on disk.

    Performance improvement over reading the full output_path:
    we identify which partitions the new batch touches and read
    only those specific sub-directories for existing IDs.
    For example, if the batch covers IT/2024, only
    gs://bucket/processed/installed_capacity/country=IT/year=2024/ is scanned.
    """
    if _path_exists(output_path):
        # Determine exactly which partition subdirs the new data will land in.
        partition_values = df.select(partition_cols).distinct().collect()
        targeted_paths = []
        for row in partition_values:
            partition_path = output_path + "/" + "/".join(
                f"{col}={row[col]}" for col in partition_cols
            )
            if _path_exists(partition_path):
                targeted_paths.append(partition_path)

        if targeted_paths:
            existing_ids = (
                spark.read.parquet(*targeted_paths)
                .select("id")
                .distinct()
            )
            df = df.join(existing_ids, on="id", how="left_anti")

    df.write.mode("append").partitionBy(*partition_cols).parquet(output_path)


def _explode_if_array(df, source_col, alias):
    dtype = df.schema[source_col].dataType
    if isinstance(dtype, ArrayType):
        return df.select("*", F.explode(F.col(source_col)).alias(alias)).drop(source_col)
    return df.select("*", F.col(source_col).alias(alias)).drop(source_col)


def process_installed_capacity(bucket_path, period_key):
    print("Starting INSTALLED CAPACITY processing")
    path = f"{bucket_path}/raw/entsoe/installed_capacity/country=*/period={period_key}/year=*/installed_capacity.xml"
    output = f"{bucket_path}/processed/installed_capacity"

    df_raw = (
        spark.read.format("xml")
        .option("rowTag", "TimeSeries")
        .load(path)
        .withColumn("country", F.regexp_extract(F.input_file_name(), r"country=([A-Z]{2})", 1))
        .withColumn("capacity_year", F.regexp_extract(F.input_file_name(), r"year=(\d{4})", 1).cast("int"))
        .filter(F.col("capacity_year").isNotNull())
        .select("country", "capacity_year", F.col("MktPSRType.psrType").alias("psrType"), "Period")
    )

    df_period = _explode_if_array(df_raw, "Period", "p")
    point_type = df_period.schema["p"].dataType["Point"].dataType

    if isinstance(point_type, ArrayType):
        df_points = df_period.select(
            "country",
            "capacity_year",
            "psrType",
            F.col("p.timeInterval.start").alias("timestamp"),
            F.explode("p.Point").alias("pt"),
        )
    else:
        df_points = df_period.select(
            "country",
            "capacity_year",
            "psrType",
            F.col("p.timeInterval.start").alias("timestamp"),
            F.col("p.Point").alias("pt"),
        )

    df = (
        df_points
        .withColumn("installed_capacity_mw", F.col("pt.quantity").cast("double"))
        .withColumn("year", F.col("capacity_year").cast("int"))
        .withColumn("id", F.sha2(F.concat_ws("|", F.col("country"), F.col("capacity_year"), F.col("psrType")), 256))
        .select("id", "timestamp", "country", "psrType", "installed_capacity_mw", "year")
    )

    _append_without_existing_ids(df, output, ["country", "year"])
    print("INSTALLED CAPACITY processing completed")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: spark-submit entsoe_installed_capacity.py <bucket_path> <start_date> <end_date>")
        sys.exit(1)

    gcs_bucket = sys.argv[1].rstrip("/")
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    period = _build_period_key(start_date, end_date)

    process_installed_capacity(gcs_bucket, period)

    print("Installed capacity processing completed")
    spark.stop()
