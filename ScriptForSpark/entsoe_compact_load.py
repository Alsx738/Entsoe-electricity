import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("ENTSOE_Load_Monthly_Compaction")
    .getOrCreate()
)


def _path_exists(path):
    hconf = spark._jsc.hadoopConfiguration()
    j_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = j_path.getFileSystem(hconf)
    return fs.exists(j_path)


def _normalize_countries(countries_arg):
    if not countries_arg or countries_arg.upper() == "ALL":
        return None
    return [c.strip().upper() for c in countries_arg.split(",") if c.strip()]


def compact_load_partitions(bucket_path, target_year, target_files, countries_arg):
    base_output = f"{bucket_path}/processed/load"
    countries = _normalize_countries(countries_arg)

    if countries is None:
        # Country auto-discovery from folder structure.
        pattern = f"{base_output}/country=*/year={target_year}"
        hconf = spark._jsc.hadoopConfiguration()
        pattern_path = spark._jvm.org.apache.hadoop.fs.Path(pattern)
        fs = pattern_path.getFileSystem(hconf)
        statuses = fs.globStatus(pattern_path)
        if not statuses:
            print(f"[INFO] No load partitions found for year={target_year}. Nothing to compact.")
            return

        discovered = []
        for status in statuses:
            path = status.getPath().toString()
            # Path segment format: .../country=IT/year=2025
            for segment in path.split("/"):
                if segment.startswith("country="):
                    discovered.append(segment.split("=", 1)[1].upper())
                    break
        countries = sorted(set(discovered))

    if not countries:
        print(f"[INFO] No countries resolved for year={target_year}. Nothing to compact.")
        return

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    compacted_count = 0
    for country in countries:
        input_partition = f"{base_output}/country={country}/year={target_year}"
        if not _path_exists(input_partition):
            print(f"[SKIP] Missing partition: country={country}, year={target_year}")
            continue

        print(f"[START] Compacting country={country}, year={target_year}")
        df = spark.read.parquet(input_partition)

        # Ensure partition columns are present and normalized.
        df = (
            df
            .withColumn("country", F.lit(country))
            .withColumn("year", F.lit(int(target_year)))
        )

        # Rewrite only touched partitions to reduce small files.
        (
            df.repartition(int(target_files))
            .write
            .mode("overwrite")
            .partitionBy("country", "year")
            .parquet(base_output)
        )

        compacted_count += 1
        print(f"[DONE] Compacted country={country}, year={target_year}")

    print(f"[COMPLETED] Compaction finished. Partitions compacted: {compacted_count}")


def main():
    parser = argparse.ArgumentParser(description="Compact ENTSOE processed load parquet partitions")
    parser.add_argument("bucket_path", help="Bucket root path, e.g. gs://my-bucket")
    parser.add_argument("year", nargs="?", help="Target year (YYYY). Defaults to current UTC year")
    parser.add_argument("target_files", nargs="?", default="4", help="Target parquet files per partition")
    parser.add_argument("--countries", default="ALL", help="Comma-separated country list or ALL")
    args = parser.parse_args()

    bucket_path = args.bucket_path.rstrip("/")
    year = int(args.year) if args.year else int(datetime.utcnow().strftime("%Y"))
    target_files = int(args.target_files)

    if target_files < 1:
        raise ValueError("target_files must be >= 1")

    compact_load_partitions(bucket_path, year, target_files, args.countries)


if __name__ == "__main__":
    main()
    spark.stop()
