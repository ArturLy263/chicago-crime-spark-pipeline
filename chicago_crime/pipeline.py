"""Run the notebook's pipeline as a batch job on a local or S3 path.

    python -m chicago_crime.pipeline --input examples/sample_crimes.csv --output output/
"""

import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession

from chicago_crime import transforms as t


def run(spark: SparkSession, input_path: str, output_path: str, as_of: date | None = None) -> dict[str, DataFrame]:
    raw = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    enriched = t.enrich(t.clean(raw, as_of=as_of))

    metrics = t.district_metrics(enriched)
    t.check_arrest_rates(metrics)

    out = output_path.rstrip("/")
    enriched.withColumnRenamed("Primary Type", "Primary_Type").write.mode(
        "overwrite"
    ).parquet(f"{out}/chicago_crimes_enriched")
    metrics.write.mode("overwrite").parquet(f"{out}/district_metrics")
    t.crime_fact(enriched).write.mode("overwrite").parquet(f"{out}/facts/crime_fact")
    t.district_dimension(enriched).write.mode("overwrite").json(
        f"{out}/dimensions/district_dimension"
    )

    return {
        "raw": raw,
        "enriched": enriched,
        "district_metrics": metrics,
        "top_districts_per_month": t.top_districts_per_month(enriched),
        "hotspots": t.hotspots(enriched),
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Chicago crime PySpark pipeline")
    parser.add_argument("--input", required=True, help="CSV file or directory (local path or s3://)")
    parser.add_argument("--output", required=True, help="output directory (local path or s3://)")
    parser.add_argument("--as-of", type=date.fromisoformat, help="reference date for days_since_crime (default: today)")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("chicago-crime-pipeline").getOrCreate()
    results = run(spark, args.input, args.output, as_of=args.as_of)

    print(f"Raw rows: {results['raw'].count():,}")
    print(f"Rows after cleaning: {results['enriched'].count():,}")
    results["district_metrics"].show(10, truncate=False)
    results["top_districts_per_month"].show(15, truncate=False)
    results["hotspots"].show(truncate=False)
    print(f"Outputs written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
