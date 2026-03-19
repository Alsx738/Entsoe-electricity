import sys
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


spark = (
    SparkSession.builder
    .appName("ENTSOE_Master_Historical_GCS")
    .getOrCreate()
)


def _path_exists(path):
    hconf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(path))


def _append_without_existing_ids(df, output_path, partition_cols):
    # Keep append semantics while avoiding duplicates when ranges overlap.
    if _path_exists(output_path):
        existing_ids = spark.read.parquet(output_path).select("id").distinct()
        df = df.join(existing_ids, on="id", how="left_anti")
    df.write.mode("append").partitionBy(*partition_cols).parquet(output_path)


def _explode_if_array(df, source_col, alias):
    dtype = df.schema[source_col].dataType
    if isinstance(dtype, ArrayType):
        return df.select("*", F.explode(F.col(source_col)).alias(alias)).drop(source_col)
    return df.select("*", F.col(source_col).alias(alias)).drop(source_col)


def process_load(bucket_path):
    print("Starting historical ACTUAL LOAD")
    path = f"{bucket_path}/raw/entsoe/historical/actual_load/*/*/load.xml"
    output = f"{bucket_path}/processed/load"

    df_raw = (
        spark.read.format("xml")
        .option("rowTag", "TimeSeries")
        .load(path)
        .filter(F.col("businessType") == "A04")
        .withColumn("country", F.regexp_extract(F.input_file_name(), r"country=([A-Z]{2})", 1))
    )

    df_period = _explode_if_array(df_raw, "Period", "p")
    point_type = df_period.schema["p"].dataType["Point"].dataType

    if isinstance(point_type, ArrayType):
        df_points = df_period.select(
            "country",
            F.col("p.timeInterval.start").alias("start"),
            F.col("p.resolution").alias("res"),
            F.explode("p.Point").alias("pt"),
        )
    else:
        df_points = df_period.select(
            "country",
            F.col("p.timeInterval.start").alias("start"),
            F.col("p.resolution").alias("res"),
            F.col("p.Point").alias("pt"),
        )

    df = (
        df_points
        .withColumn("res_min", F.regexp_extract("res", r"PT(\d+)M", 1).cast("int"))
        .withColumn("timestamp", F.expr("start + make_interval(0,0,0,0,0, (pt.position - 1) * res_min, 0)"))
        .withColumn("mw", F.col("pt.quantity").cast("double"))
        .withColumn("year", F.year("timestamp"))
        .withColumn("id", F.sha2(F.concat_ws("|", F.col("country"), F.col("timestamp")), 256))
        .select("id", "timestamp", "country", "mw", "year")
    )

    _append_without_existing_ids(df, output, ["country", "year"])
    print("Historical LOAD completed")


def process_generation(bucket_path):
    print("Starting historical GENERATION")
    path = f"{bucket_path}/raw/entsoe/historical/generation/*/*/generation.xml"
    output = f"{bucket_path}/processed/generation"

    df_raw = (
        spark.read.format("xml")
        .option("rowTag", "TimeSeries")
        .load(path)
        .withColumn("country", F.regexp_extract(F.input_file_name(), r"country=([A-Z]{2})", 1))
        .select("country", F.col("MktPSRType.psrType").alias("psrType"), "Period")
    )

    df_period = _explode_if_array(df_raw, "Period", "p")
    point_type = df_period.schema["p"].dataType["Point"].dataType

    if isinstance(point_type, ArrayType):
        df_points = df_period.select(
            "country",
            "psrType",
            F.col("p.timeInterval.start").alias("start"),
            F.col("p.resolution").alias("res"),
            F.explode("p.Point").alias("pt"),
        )
    else:
        df_points = df_period.select(
            "country",
            "psrType",
            F.col("p.timeInterval.start").alias("start"),
            F.col("p.resolution").alias("res"),
            F.col("p.Point").alias("pt"),
        )

    df = (
        df_points
        .withColumn("res_min", F.regexp_extract("res", r"PT(\d+)M", 1).cast("int"))
        .withColumn("timestamp", F.expr("start + make_interval(0,0,0,0,0, (pt.position - 1) * res_min, 0)"))
        .withColumn("mw", F.col("pt.quantity").cast("double"))
        .withColumn("year", F.year("timestamp"))
        .withColumn("id", F.sha2(F.concat_ws("|", F.col("country"), F.col("timestamp"), F.col("psrType")), 256))
        .select("id", "timestamp", "country", "psrType", "mw", "year")
    )

    _append_without_existing_ids(df, output, ["country", "year"])
    print("Historical GENERATION completed")


def process_flows(bucket_path):
    print("Starting historical PHYSICAL FLOWS")
    path = f"{bucket_path}/raw/entsoe/historical/physical_flows/*/*/*/*/flow.xml"
    output = f"{bucket_path}/processed/physical_flows"

    df_raw = (
        spark.read.format("xml")
        .option("rowTag", "TimeSeries")
        .load(path)
        .withColumn("file_path", F.input_file_name())
        .withColumn("main_country", F.regexp_extract("file_path", r"country=([A-Z]{2})", 1))
        .withColumn("direction", F.regexp_extract("file_path", r"direction=([a-z]+)", 1))
        .withColumn("border_country", F.regexp_extract("file_path", r"border=([A-Z]{2})", 1))
    )

    df_period = _explode_if_array(df_raw, "Period", "p")
    point_type = df_period.schema["p"].dataType["Point"].dataType

    if isinstance(point_type, ArrayType):
        df_points = df_period.select(
            "main_country",
            "direction",
            "border_country",
            F.col("p.timeInterval.start").alias("start"),
            F.col("p.resolution").alias("res"),
            F.explode("p.Point").alias("pt"),
        )
    else:
        df_points = df_period.select(
            "main_country",
            "direction",
            "border_country",
            F.col("p.timeInterval.start").alias("start"),
            F.col("p.resolution").alias("res"),
            F.col("p.Point").alias("pt"),
        )

    df = (
        df_points
        .withColumn("res_min", F.regexp_extract("res", r"PT(\d+)M", 1).cast("int"))
        .withColumn("timestamp", F.expr("start + make_interval(0,0,0,0,0, (pt.position - 1) * res_min, 0)"))
        .withColumn("mw", F.col("pt.quantity").cast("double"))
        .withColumn("year", F.year("timestamp"))
        .withColumn("id", F.sha2(F.concat_ws("|", F.col("main_country"), F.col("border_country"), F.col("direction"), F.col("timestamp")), 256))
        .select("id", "timestamp", "main_country", "direction", "border_country", "mw", "year")
    )

    _append_without_existing_ids(df, output, ["main_country", "year"])
    print("Historical FLOWS completed")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit entsoe_master_historical.py <bucket_path>")
        sys.exit(1)

    gcs_bucket = sys.argv[1].rstrip("/")

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(process_load, gcs_bucket),
            executor.submit(process_generation, gcs_bucket),
            executor.submit(process_flows, gcs_bucket),
        ]
        for future in futures:
            future.result()

    print("Historical processing completed")
    spark.stop()