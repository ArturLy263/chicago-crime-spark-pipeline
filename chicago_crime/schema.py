"""Explicit schema of the "Crimes - 2001 to Present" CSV export.

Columns and order follow the export (the 22 columns printed in the notebook).
Numeric types match what schema inference chose on the full file in the EMR
run; `IUCR` and `FBI Code` stay strings because they have leading zeros and
letters ("0910", "08B"), and `Date` / `Updated On` stay strings because they
are parsed later with an explicit format.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

RAW_SCHEMA = StructType(
    [
        StructField("ID", IntegerType()),
        StructField("Case Number", StringType()),
        StructField("Date", StringType()),
        StructField("Block", StringType()),
        StructField("IUCR", StringType()),
        StructField("Primary Type", StringType()),
        StructField("Description", StringType()),
        StructField("Location Description", StringType()),
        StructField("Arrest", BooleanType()),
        StructField("Domestic", BooleanType()),
        StructField("Beat", IntegerType()),
        StructField("District", IntegerType()),
        StructField("Ward", IntegerType()),
        StructField("Community Area", IntegerType()),
        StructField("FBI Code", StringType()),
        StructField("X Coordinate", IntegerType()),
        StructField("Y Coordinate", IntegerType()),
        StructField("Year", IntegerType()),
        StructField("Updated On", StringType()),
        StructField("Latitude", DoubleType()),
        StructField("Longitude", DoubleType()),
        StructField("Location", StringType()),
    ]
)


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read the CSV export with the explicit schema.

    A CSV schema is applied by position, so the header is checked first: a
    file with other columns or another order raises ValueError instead of
    being read into the wrong columns. A value that does not fit its type
    (e.g. a non-numeric District) becomes NULL (PERMISSIVE mode).
    """
    header = spark.read.option("header", True).csv(path).columns
    if header != RAW_SCHEMA.fieldNames():
        raise ValueError(
            f"Unexpected CSV header in {path}: expected {RAW_SCHEMA.fieldNames()}, got {header}"
        )
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(path)
    )
