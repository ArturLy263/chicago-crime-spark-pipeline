# Chicago Crime Analysis with PySpark on AWS EMR

[![CI](https://github.com/ArturLy263/chicago-crime-spark-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/ArturLy263/chicago-crime-spark-pipeline/actions/workflows/ci.yml)

PySpark analysis of the full **Chicago Crimes – 2001 to Present** dataset (8.5M rows), run once as a
notebook on an AWS EMR cluster provided by my data-engineering mentorship programme. The same logic
is also available as a Python package with a command-line job and unit tests that run locally on a
small synthetic sample, without AWS.

| What | Where | Verified by |
| --- | --- | --- |
| Original EMR run on the full dataset | [`chicago_crime_analysis.ipynb`](chicago_crime_analysis.ipynb) | outputs saved in the notebook (not re-run since) |
| Same transformations as functions | [`chicago_crime/transforms.py`](chicago_crime/transforms.py) | 21 pytest tests on synthetic data, GitHub Actions |
| Batch job (local path or `s3://`) | [`chicago_crime/pipeline.py`](chicago_crime/pipeline.py), explicit input schema in [`schema.py`](chicago_crime/schema.py) | end-to-end test on [`examples/sample_crimes.csv`](examples/sample_crimes.csv) |
| Running the job on EMR | [`docs/emr-runbook.md`](docs/emr-runbook.md): permissions, commands, evidence to keep | **not executed yet** |

## Pipeline

```mermaid
flowchart LR
    Raw[(raw CSV<br/>S3 or local)] --> Clean[clean<br/>coordinates, dates, duplicates]
    Clean --> Enrich[enrich<br/>time of day, violent flag, district name]
    Enrich --> Enr[(chicago_crimes_enriched<br/>Parquet)]
    Enrich --> Metrics[district metrics<br/>+ arrest-rate check]
    Metrics --> M[(district_metrics<br/>Parquet)]
    Enrich --> Fact[(facts/crime_fact<br/>Parquet)]
    Enrich --> Dim[(dimensions/district_dimension<br/>JSON)]
    Enrich --> Win[window functions, hotspots<br/>printed]
```

| Stage | What happens | Result of the EMR run (from the notebook) |
| --- | --- | --- |
| Ingest | Notebook: schema inference. Job: explicit 22-column schema, header checked | **8,548,383 rows**, 22 columns |
| Profile | Classify columns by type; null percentage per column (notebook only) | e.g. Ward 7.19 % null, coordinates 1.13 % null |
| Clean | Drop rows without coordinates or with an unparseable `Date`, de-duplicate on `ID` and then `Case Number`, cast `Arrest` / `Domestic` to boolean, add `days_since_crime` | 8,451,823 rows after the coordinate filter, **8,451,207** after de-duplication |
| Enrich | `time_of_day` (Night 0–5 h, Morning 6–11, Afternoon 12–17, Evening 18–23), `is_violent_crime` (homicide, criminal sexual assault, battery), `district_name` (unknown codes → `Unknown`) | 1,574,913 rows flagged violent |
| Aggregate | Total crimes, arrests and arrest rate per district | district table |
| Quality check | Stop with an error if any district has a negative arrest rate | check passed |
| Window functions | Days since the previous crime in the same district (`lag`); top 3 districts per month (`dense_rank`) | ranked tables |
| Model | `crime_fact` as Parquet, `district_dimension` as JSON | written to S3 |
| Hotspots | Crimes per cell of coordinates rounded to 3 decimals (roughly 100 m) | top 5 cells |

The notebook also contains written answers on Spark concepts: driver vs executors, data skew,
`repartition` vs `coalesce`, Parquet vs JSON, and Spot vs On-Demand instances.

## Run it locally

Requirements: Python 3.12 or 3.14 and Java 17 or later. Commands run from the repository root.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt        # requirements.txt alone is enough to run the job

python -m pytest tests -q

python -m chicago_crime.pipeline \
    --input examples/sample_crimes.csv --output output --as-of 2024-03-01
```

The job prints the row counts, the district metrics, the top districts per month and the hotspots,
and writes:

```text
output/
├── chicago_crimes_enriched/          # cleaned + enriched rows, Parquet, one folder per Year=…
├── district_metrics/                 # Parquet
├── facts/crime_fact/                 # Parquet
└── dimensions/district_dimension/    # JSON
```

`--as-of` fixes the reference date for `days_since_crime`; without it, today's date is used, as in
the notebook.

**Sample data.** [`examples/sample_crimes.csv`](examples/sample_crimes.csv) has 13 hand-written,
synthetic rows in the export's 22-column layout. It contains one row without coordinates, one with
an invalid date, an exact duplicate and a repeated case number, so 9 rows survive cleaning. It is
not real crime data.

**Input schema.** The job reads the CSV with an explicit schema (`chicago_crime/schema.py`) instead
of inferring it: no extra pass over 8.5M rows, and stable types. Numeric types match what inference
chose on the full file in the EMR run; `IUCR` and `FBI Code` stay strings ("0910", "08B"). A CSV
schema is applied by position, so the job first compares the header with the schema and stops on
any difference. A value that does not fit its type (e.g. a non-numeric `District`) becomes NULL and
the rest of the row is kept.

**Partitioning.** `chicago_crimes_enriched` is written partitioned by `Year` (2001 to today, about
26 folders). Most questions about this data are bounded in time (a month, a year), and Spark then
reads only the matching folders. The data is repartitioned by `Year` before writing, so each year
gets one file — with 8.5M rows that is roughly 300k rows per file, not thousands of small files.
The other outputs stay unpartitioned: `district_metrics` and `district_dimension` have a few dozen
rows, and `crime_fact` has no date column to partition by (see Limitations).

## Run it on the full dataset

1. Download the dataset as CSV from the City of Chicago Data Portal (link below).
2. Put it in an S3 location you control, or keep it locally.
3. Run the job with your own paths, e.g. on an EMR cluster (see
   [`docs/emr-runbook.md`](docs/emr-runbook.md) for permissions, EMR Serverless and a safe first run):

   ```bash
   spark-submit --py-files chicago_crime.zip chicago_crime/pipeline.py \
       --input s3://<your-bucket>/chicago_crime/raw/ --output s3://<your-bucket>/chicago_crime/
   ```

   (`chicago_crime.zip` is the `chicago_crime/` folder zipped.) Or run the notebook with a Sparkmagic
   kernel after replacing `s3://<your-bucket>/…` with your locations.

The packaged job has only been run locally on the synthetic sample (with `python -m` and with
`spark-submit --master "local[1]"`); it has not been run on EMR or on the full dataset. The figures
above come from the notebook run.

## Tests and CI

`tests/test_pipeline.py` checks, on the sample file: which rows are dropped by cleaning and why,
column types, `days_since_crime` with a fixed date, `time_of_day` boundaries, the violent-crime
flag, district names including the `Unknown` fallback, district metrics and the arrest-rate check,
the `lag` and `dense_rank` window results, the hotspot cell, the fact and dimension tables, the
explicit schema (codes kept as strings, wrong header rejected, a mistyped value becoming NULL), and
that the job writes all four outputs with one `Year=` folder and one file per year.

GitHub Actions runs the tests on Python 3.12 and 3.14 with PySpark 4.1, and on Python 3.9 with
PySpark 3.5 — the versions in Amazon EMR 7.x — all with Java 17. No AWS access or secrets are needed.

## Environment and provenance

- The notebook ran on AWS EMR with a Sparkmagic (PySpark) kernel; the session was configured with
  1 executor (1 core, 2 GB memory).
- The data was read from and written to an S3 bucket owned by the mentorship programme. It is not
  publicly readable, so after the run its name was replaced with `<your-bucket>` everywhere in the
  notebook, including the printed output paths, and the EMR Serverless application and job-run IDs
  in the session output were replaced with `<application-id>` and `<job-run-id>`; nothing else in the
  code or outputs was changed.
- The saved outputs come from one Spark session; the execution counts show that a few cells were
  re-run during the session. I have not re-run the notebook since.
- Two differences between the package and the notebook: dates are parsed with `try_to_timestamp`,
  so invalid dates become NULL and are dropped (plain `to_timestamp` raises an error under Spark 4's
  default ANSI mode), and the reference date for `days_since_crime` can be fixed with `--as-of`.

## Limitations and what I would improve

- `days_since_crime` depends on the run date, so it should not live in a fact table; the event date
  should be stored instead.
- `district_dimension` includes a `NULL` district, and `dropDuplicates` keeps an arbitrary row per
  key; data-quality checks are minimal and should cover row counts between stages and key uniqueness.
- The fact and dimension tables are written separately; nothing joins them yet.
- Partitions use the export's own `Year` column; the job does not check that it matches the year of
  `Date`.

## Dataset

[Crimes – 2001 to Present](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/about_data),
City of Chicago Data Portal.
