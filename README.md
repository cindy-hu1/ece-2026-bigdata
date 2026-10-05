# ece-2026-bigdata

Lab work for the Big Data course (ECE, 2026).

| Lab | Topic | Where |
| --- | --- | --- |
| Lab 1 | Python project with uv, random dataset generator | this folder: [`src/`](src/), [`pyproject.toml`](pyproject.toml) |
| Lab 2 | Object storage with S3, upload from a Kubernetes Job | [`lab-2-s3/`](lab-2-s3/README.md) |

## Lab 1: Dataset generator (uv)

This project generates a random dataset consisting of users and orders. Scripts are written in Python and the project
uses [uv](https://docs.astral.sh/uv/).

### Usage

```bash
uv run dataset-users -h
#> usage: dataset-users [-h] [-c COUNT] [-o {csv,json,jsonline}]
uv run dataset-orders -h
#> usage: dataset-orders [-h] [-C COUNT_MIN] [-c COUNT_MAX] [-d DATE_FROM] [-o {csv,json,jsonline}] [-u COUNT_USERS]
```

Generate the datasets used by the next labs:

```bash
uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
```

The generated CSV files are data, not source code: they are ignored by Git (`*.csv` in `.gitignore`).

## Lab 2: Object storage with S3

The datasets of Lab 1 are uploaded to the bronze layer of an S3 bucket from a Kubernetes Job.

- Walkthrough, result and issues encountered: [`lab-2-s3/README.md`](lab-2-s3/README.md)
- Answers to the questions of the lab: [`lab-2-s3/answers.md`](lab-2-s3/answers.md)
- Job manifest: [`lab-2-s3/job-upload-bronze.yaml`](lab-2-s3/job-upload-bronze.yaml)