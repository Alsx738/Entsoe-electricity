import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


spark = (
    SparkSession.builder
    .appName("ENTSOE_Master_Historical_GCS")
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


def _glob_has_match(path_pattern):
    hconf = spark._jsc.hadoopConfiguration()
    j_path = spark._jvm.org.apache.hadoop.fs.Path(path_pattern)
    fs = j_path.getFileSystem(hconf)
    matches = fs.globStatus(j_path)
    return matches is not None and len(matches) > 0


def _append_without_existing_ids(df, output_path, partition_cols):
    """Append new records while skipping IDs already present on disk.

    Performance improvement over reading the full output_path:
    we identify which partitions the new batch touches and read
    only those specific sub-directories for existing IDs.
    For example, if the batch covers IT/2024, only
    gs://bucket/processed/load/country=IT/year=2024/ is scanned.
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


def _point_value_col(df, point_col="pt"):
    """Resolve the value field used by ENTSO-E points across document types."""
    field_names = {f.name for f in df.schema[point_col].dataType.fields}
    if "quantity" in field_names:
        return F.col(f"{point_col}.quantity").cast("double")
    if "price.amount" in field_names:
        return F.col(f"{point_col}.`price.amount`").cast("double")
    if "price_amount" in field_names:
        return F.col(f"{point_col}.price_amount").cast("double")
    if "price" in field_names:
        return F.col(f"{point_col}.price.amount").cast("double")
    raise ValueError("Unsupported Point schema: expected quantity or price amount field")


def process_load(bucket_path, period_key):
    print("Starting historical ACTUAL LOAD")
    path = f"{bucket_path}/raw/entsoe/historical/actual_load/country=*/period={period_key}/*/load.xml"
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


def process_generation(bucket_path, period_key):
    print("Starting historical GENERATION")
    path = f"{bucket_path}/raw/entsoe/historical/generation/country=*/period={period_key}/*/generation.xml"
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


def process_flows(bucket_path, period_key):
    print("Starting historical PHYSICAL FLOWS")
    path = f"{bucket_path}/raw/entsoe/historical/physical_flows/country=*/direction=*/border=*/period={period_key}/*/flow.xml"
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


def process_prices(bucket_path, start_date, end_date):
    print("Starting historical PRICES")
    start_year = datetime.strptime(start_date, "%Y-%m-%d").year
    end_year = datetime.strptime(end_date, "%Y-%m-%d").year

    path = f"{bucket_path}/raw/entsoe/historical/prices/country=*/year=*/*.xml"
    if not _glob_has_match(path):
        print("No historical PRICES files found for selected period, skipping PRICES processing")
        return

    output = f"{bucket_path}/processed/prices"

    df_raw = (
        spark.read.format("xml")
        .option("rowTag", "TimeSeries")
        .load(path)
        .withColumn("country", F.regexp_extract(F.input_file_name(), r"country=([A-Z]{2})", 1))
        .withColumn("file_year", F.regexp_extract(F.input_file_name(), r"year=(\d{4})", 1).cast("int"))
        .filter((F.col("file_year") >= F.lit(start_year)) & (F.col("file_year") <= F.lit(end_year)))
    )

    if df_raw.rdd.isEmpty():
        print("No historical PRICES files within selected years, skipping PRICES processing")
        return

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
        .withColumn("price_eur_mwh", _point_value_col(df_points, "pt"))
        .withColumn("year", F.year("timestamp"))
        .withColumn("id", F.sha2(F.concat_ws("|", F.col("country"), F.col("timestamp")), 256))
        .select("id", "timestamp", "country", "price_eur_mwh", "year")
    )

    _append_without_existing_ids(df, output, ["country", "year"])
    print("Historical PRICES completed")


def process_installed_capacity(bucket_path, period_key):
    print("Starting historical INSTALLED CAPACITY")
    path = f"{bucket_path}/raw/entsoe/installed_capacity/country=*/period={period_key}/year=*/installed_capacity.xml"
    output = f"{bucket_path}/processed/installed_capacity"

    df_raw = (
        spark.read.format("xml")
        .option("rowTag", "TimeSeries")
        .load(path)
        .withColumn("country", F.regexp_extract(F.input_file_name(), r"country=([A-Z]{2})", 1))
        .withColumn("capacity_year", F.regexp_extract(F.input_file_name(), r"year=(\d{4})", 1).cast("int"))
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
    print("Historical INSTALLED CAPACITY completed")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: spark-submit entsoe_master_historical.py <bucket_path> <start_date> <end_date>")
        sys.exit(1)

    gcs_bucket = sys.argv[1].rstrip("/")
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    period = _build_period_key(start_date, end_date)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(process_load, gcs_bucket, period),
            executor.submit(process_generation, gcs_bucket, period),
            executor.submit(process_flows, gcs_bucket, period),
            executor.submit(process_prices, gcs_bucket, start_date, end_date),
            executor.submit(process_installed_capacity, gcs_bucket, period),
        ]
        for future in futures:
            future.result()

    print("Historical processing completed")
    spark.stop()