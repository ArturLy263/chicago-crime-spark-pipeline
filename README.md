# Chicago Crime Analysis Pipeline on AWS EMR

Distributed PySpark data engineering pipeline for analyzing the Chicago Crime dataset on AWS EMR.

## Project Overview

This project processes and analyzes more than 8 million Chicago crime records using PySpark and AWS EMR.

The pipeline includes:
- data cleaning
- feature engineering
- temporal analytics
- geospatial hotspot detection
- parquet optimization
- Spark window functions
- fact/dimension modeling
- data quality validation

## Tech Stack

- PySpark
- AWS EMR
- Amazon S3
- Parquet
- Spark SQL
- Jupyter Notebook

## Features

- Distributed processing of large-scale datasets
- Crime hotspot analysis
- Temporal ranking with window functions
- Partitioned parquet outputs
- Data quality checks
- Schema evolution simulation
- Multi-format outputs (JSON + Parquet)

## Pipeline Flow

S3 Raw Data
    ↓
PySpark Cleaning
    ↓
Feature Engineering
    ↓
Aggregation & Analytics
    ↓
Parquet Output
    ↓
S3 Storage

## Dataset

Chicago Crimes 2001–Present

Source:
https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/about_data