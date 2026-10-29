"""Tests on examples/sample_crimes.csv: 13 synthetic rows in the export's 22-column layout.

Row 9 has no coordinates, row 10 has an unparseable date, one row repeats ID 1
and ID 12 repeats case number JA100002, so 9 rows survive cleaning.
"""

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from chicago_crime import transforms as t
from chicago_crime.pipeline import run
from chicago_crime.schema import RAW_SCHEMA, read_raw

SAMPLE = str(Path(__file__).resolve().parent.parent / "examples" / "sample_crimes.csv")
AS_OF = date(2024, 3, 1)


@pytest.fixture(scope="module")
def raw(spark):
    return read_raw(spark, SAMPLE)


@pytest.fixture(scope="module")
def enriched(raw):
    return t.enrich(t.clean(raw, as_of=AS_OF)).cache()


def by_case(df):
    return {r["Case Number"]: r for r in df.collect()}


def test_cleaning_drops_invalid_and_duplicate_rows(raw, enriched):
    assert raw.count() == 13
    assert enriched.count() == 9
    cases = set(by_case(enriched))
    assert "JA100009" not in cases  # no coordinates
    assert "JA100010" not in cases  # date not parseable
    assert enriched.select("ID").distinct().count() == 9


def test_explicit_schema_keeps_codes_as_strings(raw):
    assert raw.schema == RAW_SCHEMA
    first = raw.filter("ID = 3").first()
    assert (first["IUCR"], first["FBI Code"], first["District"], first["Year"]) == ("0110", "01A", 7, 2024)


def test_unexpected_header_is_rejected(spark, tmp_path):
    path = tmp_path / "other.csv"
    path.write_text("Case Number,ID\nJA1,1\n")
    with pytest.raises(ValueError, match="Unexpected CSV header"):
        read_raw(spark, str(path))


def test_value_of_wrong_type_becomes_null(spark, tmp_path):
    lines = Path(SAMPLE).read_text().splitlines()
    lines[1] = lines[1].replace(",101,1,10,", ",101,abc,10,")  # District of ID 1
    path = tmp_path / "bad.csv"
    path.write_text("\n".join(lines) + "\n")
    row = read_raw(spark, str(path)).filter("Beat = 101 AND `Case Number` = 'JA100001'").orderBy("District").first()
    assert row["District"] is None
    assert row["Arrest"] is True  # the rest of the row is kept


def test_types_after_cleaning(enriched):
    types = dict(enriched.dtypes)
    assert types["Date"] == "timestamp"
    assert types["Arrest"] == "boolean"
    assert types["Domestic"] == "boolean"


def test_days_since_crime_uses_reference_date(enriched):
    assert by_case(enriched)["JA100001"]["days_since_crime"] == 56


@pytest.mark.parametrize(
    "case, expected",
    [
        ("JA100001", "Night"),      # 02:15
        ("JA100013", "Night"),      # 00:30
        ("JA100006", "Morning"),    # 06:00
        ("JA100007", "Afternoon"),  # 12:00
        ("JA100008", "Afternoon"),  # 17:59
        ("JA100004", "Evening"),    # 20:00
    ],
)
def test_time_of_day_boundaries(enriched, case, expected):
    assert by_case(enriched)[case]["time_of_day"] == expected


def test_violent_flag(enriched):
    violent = {c for c, r in by_case(enriched).items() if r["is_violent_crime"]}
    assert violent == {"JA100001", "JA100003", "JA100006", "JA100007"}


def test_district_names_with_unknown_fallback(enriched):
    rows = by_case(enriched)
    assert rows["JA100001"]["district_name"] == "Central"
    assert rows["JA100005"]["district_name"] == "Grand Central"
    assert rows["JA100008"]["district_name"] == "Unknown"  # district 13 is not in the mapping
    assert rows["JA100013"]["district_name"] == "Unknown"  # no district


def test_district_metrics(enriched):
    metrics = {r["District"]: r for r in t.district_metrics(enriched).collect()}
    assert (metrics[1]["total_crimes"], metrics[1]["total_arrests"], metrics[1]["arrest_rate"]) == (3, 1, 0.3333)
    assert (metrics[7]["total_crimes"], metrics[7]["total_arrests"], metrics[7]["arrest_rate"]) == (3, 2, 0.6667)
    t.check_arrest_rates(t.district_metrics(enriched))


def test_check_arrest_rates_rejects_negative_rates(spark):
    bad = spark.createDataFrame([(1, -0.5)], ["District", "arrest_rate"])
    with pytest.raises(ValueError, match="negative Arrest Rate"):
        t.check_arrest_rates(bad)


def test_days_since_previous_crime_per_district(enriched):
    rows = (
        t.days_since_previous_crime(enriched)
        .filter(F.col("District") == 7)
        .orderBy("Date")
        .select("days_since_last_crime")
        .collect()
    )
    assert [r[0] for r in rows] == [None, 8, 13]


def test_top_districts_per_month(enriched):
    january = {
        (r["District"], r["monthly_crime_rank"])
        for r in t.top_districts_per_month(enriched).filter("year = 2024 AND month = 1").collect()
    }
    assert january == {(1, 1), (7, 1), (25, 2)}


def test_hotspots(enriched):
    top = t.hotspots(enriched).first()
    assert (top["geohash_cell"], top["count"]) == ("41.881,-87.627", 3)


def test_fact_and_dimension_tables(enriched):
    fact = t.crime_fact(enriched)
    assert fact.columns == ["Case Number", "District_ID", "Arrest", "Domestic", "is_violent_crime", "days_since_crime"]
    assert fact.count() == 9
    dim = {r["District_ID"]: r["district_name"] for r in t.district_dimension(enriched).collect()}
    assert dim == {1: "Central", 7: "Englewood", 13: "Unknown", 25: "Grand Central", None: "Unknown"}


def test_run_writes_all_outputs(spark, tmp_path):
    run(spark, SAMPLE, str(tmp_path), as_of=AS_OF)
    enriched = spark.read.parquet(str(tmp_path / "chicago_crimes_enriched"))
    assert enriched.count() == 9
    assert "Primary_Type" in enriched.columns
    assert enriched.filter("Year = 2024").count() == 9  # partition column read back

    year_dirs = [p for p in (tmp_path / "chicago_crimes_enriched").iterdir() if p.is_dir()]
    assert [p.name for p in year_dirs] == ["Year=2024"]
    assert len(list(year_dirs[0].glob("*.parquet"))) == 1  # one file per year
    assert spark.read.parquet(str(tmp_path / "district_metrics")).count() == 5
    assert spark.read.parquet(str(tmp_path / "facts" / "crime_fact")).count() == 9
    assert spark.read.json(str(tmp_path / "dimensions" / "district_dimension")).count() == 5
