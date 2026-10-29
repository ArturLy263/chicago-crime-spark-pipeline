# Running the packaged job on Amazon EMR — runbook

Status: **not executed yet.** The commands below have not been run against AWS. What has been
verified locally: the job and its tests on Python 3.12/3.14 with PySpark 4.1 and on Python 3.9 with
PySpark 3.5 (the versions in EMR 7.x), and the `spark-submit --py-files` packaging with
`--master local[1]` on the synthetic sample.

Running it creates AWS charges. Do it only with an account, bucket and role you are allowed to use,
and start with one year of data.

## 1. Requirements

- EMR release with **Spark 3.5 or later** (EMR 7.x), because the job uses `try_to_timestamp`.
  EMR Serverless or EMR on EC2 both work; the mentorship notebook ran on EMR.
- The CSV export of "Crimes – 2001 to Present" in S3 with the original 22-column header; the job
  rejects any other header.
- An S3 prefix for the code and one for the output.

Placeholders used below — replace them, never commit real values:

| Placeholder | Meaning |
| --- | --- |
| `<your-bucket>` | S3 bucket for code, input and output |
| `<account-id>` | AWS account ID |
| `<application-id>` | EMR Serverless application ID |
| `<runtime-role>` | IAM role the job runs as |

## 2. Minimal permissions for the job's runtime role

Read the code and input, write and overwrite the output (overwrite deletes the previous files),
and nothing else. EMR adds the CloudWatch / S3 log permissions it needs through its own setup;
keep logs in a prefix of the same bucket if you use S3 logging.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListOnlyTheProjectPrefixes",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::<your-bucket>",
      "Condition": {"StringLike": {"s3:prefix": ["chicago_crime", "chicago_crime/*"]}}
    },
    {
      "Sid": "ReadCodeAndInput",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::<your-bucket>/chicago_crime/code/*",
        "arn:aws:s3:::<your-bucket>/chicago_crime/raw/*"
      ]
    },
    {
      "Sid": "WriteOutput",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::<your-bucket>/chicago_crime/output/*"
    }
  ]
}
```

The person starting the run additionally needs `emr-serverless:StartJobRun` /
`GetJobRun` on the application (or `elasticmapreduce:AddJobFlowSteps` on the cluster) and
`iam:PassRole` for `<runtime-role>` only.

## 3. Package and upload

```bash
zip -r chicago_crime.zip chicago_crime -x '*__pycache__*'
aws s3 cp chicago_crime.zip              s3://<your-bucket>/chicago_crime/code/
aws s3 cp chicago_crime/pipeline.py      s3://<your-bucket>/chicago_crime/code/
```

## 4. Start one run

Parameters: `--input` (file or prefix), `--output` (prefix, overwritten), `--as-of` (fixes
`days_since_crime`, so the run can be compared with later runs).

EMR Serverless:

```bash
aws emr-serverless start-job-run \
  --application-id <application-id> \
  --execution-role-arn arn:aws:iam::<account-id>:role/<runtime-role> \
  --job-driver '{"sparkSubmit": {
      "entryPoint": "s3://<your-bucket>/chicago_crime/code/pipeline.py",
      "entryPointArguments": ["--input", "s3://<your-bucket>/chicago_crime/raw/",
                              "--output", "s3://<your-bucket>/chicago_crime/output/",
                              "--as-of", "2026-11-01"],
      "sparkSubmitParameters": "--py-files s3://<your-bucket>/chicago_crime/code/chicago_crime.zip"}}'
```

EMR on EC2 (existing cluster):

```bash
aws emr add-steps --cluster-id <cluster-id> --steps Type=Spark,Name=chicago-crime,ActionOnFailure=CONTINUE,Args=[--py-files,s3://<your-bucket>/chicago_crime/code/chicago_crime.zip,s3://<your-bucket>/chicago_crime/code/pipeline.py,--input,s3://<your-bucket>/chicago_crime/raw/,--output,s3://<your-bucket>/chicago_crime/output/,--as-of,2026-11-01]
```

Safe order: first a single year (copy one year's rows into its own prefix and point `--input` at
it), check the outputs, then the full file. Set a maximum capacity on the Serverless application
and stop or terminate whatever you started afterwards.

## 5. What to record as evidence

Save these in `docs/runs/<date>.md` (no account IDs, bucket names or ARNs):

- EMR release, Spark and Python versions; job run or step ID (redacted if needed)
- input: number of objects and total size
- the job's printed counts: raw rows and rows after cleaning (the notebook run had 8,548,383 and
  8,451,207 — differences are expected if the export has been refreshed since)
- district metrics, top districts per month and hotspots tables as printed
- outputs: number of `Year=` partitions, files and total size per output folder
  (`aws s3 ls --recursive --summarize`)
- run duration and the capacity used (vCPU-hours / memory-GB-hours from the job run details)
- any warnings or errors from the driver log

Only after such a record exists should the README say the packaged job ran on EMR.
