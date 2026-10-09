# Lab 3: SQL analytics with DuckDB

The SQL queries of the exercises are in [exercises.sql](exercises.sql). The Python report is in `src/work/orders_report.py`.

## S3 configuration

**What are the risks of storing the persistent secret in the home directory, and why are they limited on Onyxia?**

The secret file contains the access key and the session token in clear text. Anyone or anything able to read the home directory can reuse them: another process, a backup, a copy of the folder, or an accidental commit if the file ends up in the project. On Onyxia the risk is limited because the credentials are temporary and expire after a few hours, they only give access to the user's own bucket, and the home directory lives in a personal volume of a personal service, outside of the Git repository.

**How would you restrict the access to S3 for a Kubernetes Job?**

The Job should not reuse the personal secret. It gets its own credentials, stored in a Kubernetes Secret and injected only into that Job as environment variables, or better, short-lived credentials obtained from its service account (workload identity). The associated policy follows the least privilege principle, for example read-only on the `bronze/` prefix and write access on the output prefix only. Inside the Job, the DuckDB secret is created in memory with `CREATE SECRET` and is never persisted to disk.

## Query the bronze layer

**Why does `approx_unique` return 40 and 3167 instead of 50 and 2829?**

`approx_unique` does not count the distinct values exactly. It uses a probabilistic algorithm (HyperLogLog) which estimates the cardinality with a small amount of memory. The result is fast but comes with an estimation error, visible here on both columns. An exact count requires `count(DISTINCT ...)`.

**Which issue may occur with a large file whose first rows are not representative?**

DuckDB infers the types from a sample. If the first rows of a column only contain integers and a later row contains `3.5` or `N/A`, the column is typed `BIGINT` and the read fails with a conversion error. In the same way, a column may be inferred as text when it should be a date. The fix is to read the whole file for the detection with `sample_size = -1`, or to declare the types explicitly with the `columns` or `types` options.

## Parquet export

**Why is the `uuid` column barely compressed?**

Every value is a different random identifier. There is no repetition to exploit: dictionary encoding brings nothing since all the values are distinct, and Snappy finds no recurring patterns in random characters.

**Why is the compressed size of some columns larger than their uncompressed size?**

When the data is already compact, as for `date` (binary timestamps without repetition) or `product` (already dictionary encoded), Snappy cannot reduce it further. It still adds its own headers, so the compressed result is a few bytes larger than the input.

## Hive partitioning

The `orders` table was written with `PARTITION_BY (product)`, which creates one `product=<value>/` directory per product, 6 files in total. Results of `EXPLAIN ANALYZE SELECT count(*) ... WHERE product = 'cookie'`:

| Metric | Value |
|---|---|
| Files scanned | 1/6 |
| Data read | 33.7 KiB |
| GET requests | 3 |
| Rows read | 466 |
| Total time | 0.119 s |

The filter is applied to the file paths (`File Filters`) before reading anything. Only `product=cookie/data_0.parquet` is opened. The 466 rows match the sum of the monthly cookie orders of the report (128 + 121 + 122 + 95).

**Why is partitioning by `uuid` a bad idea?**

Each order has a unique `uuid`, so the dataset would be split into 2829 directories holding a single row each. Object storage handles a large number of tiny objects poorly: every object costs at least one HTTP request with its own latency, listing the prefix becomes slow, and the Parquet footer and metadata end up larger than the data itself. A query reading the whole dataset would send thousands of requests instead of a few. Moreover, no query filters on a single `uuid` value in practice, so the pruning would never be useful.

**Which partition column would you choose for a dataset of orders growing every day?**

The order date, for example one partition per day or per month depending on the volume. New orders are written into new partitions without rewriting the existing files, which fits object storage where objects are immutable. Most analytical queries filter on a time range, so they only read the relevant partitions. The granularity must be chosen to avoid many tiny files: with a low daily volume, partitioning by month is better.

## CSV vs. Parquet at scale

The generated dataset contains 252,416 rows and its dates stop in 2048, while the instructions mention 999,490 rows over more than a century. The absolute values are smaller but the conclusions are the same.

| Metric | CSV | Parquet |
|---|---|---|
| Size on S3 | 29.6 MB | 11.5 MB |
| `GROUP BY product`, data read | 28.2 MiB | 203.5 KiB |
| `GROUP BY product`, time | 1.13 s | 0.533 s |
| `date >= '2100-01-01'` filter, data read | | 16 KiB (1 GET) |
| `date >= '2100-01-01'` filter, time | | 0.267 s |

Row groups of the `date` column:

| row_group_id | Rows | stats_max |
|---|---|---|
| 0 | 123,573 | 2034-02-04 |
| 1 | 124,276 | 2048-04-10 |
| 2 | 4,567 | 2048-10-17 |

**How many row groups does the file contain, and how many are skipped by the filter?**

The file contains 3 row groups, and all 3 are skipped. Their `stats_max` is at most 2048-10-17, so none of them can contain a date after 2100. DuckDB only reads the footer (16 KiB in a single GET request) and returns 0 without reading the `date` column. With the larger dataset of the instructions, the last row groups reach dates after 2100 and must be read.

**What would happen if the orders were shuffled?**

Each row group would contain dates from the whole period. The min/max intervals of all the row groups would overlap, none could be skipped, and DuckDB would have to read the full `date` column. Statistics-based pruning only works when the data is sorted or clustered on the filtered column.

**Which part of the difference is due to the network, which part to the CSV parsing?**

The CSV query downloads the whole file (28.2 MiB) to use 2 columns, while Parquet only reads the footer and the `product` and `quantity` column chunks, about 140 times less data. The filtered Parquet query (0.267 s for 16 KiB) gives an estimate of the fixed cost: process startup, connection and S3 requests. Removing it, the CSV query spends about 0.9 s transferring 28 MiB and mostly parsing text (splitting lines, converting dates and integers), while Parquet spends about 0.26 s decoding 203 KiB of binary data. The S3 server is on the same network, so the network share stays small here. On a slower or busier network, the gap would widen in favor of Parquet since it transfers far less data.

## Exercises

The queries are in [exercises.sql](exercises.sql).

**1. Average number of orders per user, and average quantity per order**

| avg_orders_per_user | avg_quantity_per_order |
|---|---|
| 56.58 | 3.05 |

2829 orders for 50 users, and 8615 units sold over 2829 orders.

**2. First and last order of each user, and number of days between them**

Every user orders over a very short period, between 0 and 5 days. The 5 longest spans:

| username | first_order | last_order | days_between |
|---|---|---|---|
| hoganashlee | 2020-02-06 | 2020-02-11 | 5 |
| elizabeth57 | 2020-02-15 | 2020-02-19 | 4 |
| wrightjames | 2020-01-08 | 2020-01-12 | 4 |
| ahancock | 2020-03-02 | 2020-03-06 | 4 |
| yobrien | 2020-02-22 | 2020-02-26 | 4 |

The generator seems to give each user a block of close orders instead of spreading them over the 4 months.

**3. Hour of the day with the highest quantity sold**

| hour | quantity |
|---|---|
| 6 | 385 |
| 15 | 382 |
| 0 | 379 |

6 a.m. UTC comes first with 385 units. The gaps are tiny because the generator places exactly one order per hour with a random quantity, so no hour really stands out.

**4. Month-over-month variation of the quantity sold, in percent**

| Product | 2020-02 | 2020-03 | 2020-04 |
|---|---|---|---|
| bread | +14.9 | +13.5 | -19.2 |
| brioche | -18.8 | -4.9 | -6.9 |
| cookie | -1.3 | -8.4 | -18.2 |
| croissant | +8.9 | -1.8 | +0.3 |
| donut | -28.3 | +2.2 | -1.2 |
| drink | -3.2 | +10.1 | -13.2 |

January has no previous month, so `lag` returns `NULL`. The drop in April is partly an artifact: the last order is dated April 27, so the month is incomplete.

**5. Users who ordered every product at least once**

45 users out of 50 ordered each of the 6 products. The 5 others never ordered at least one of them.

## Python API

The `orders_report.py` module reads `bronze/orders.csv` from S3 with DuckDB and aggregates the orders per month. The product is passed as a query parameter, `$product`, which prevents SQL injection. The command is declared in `pyproject.toml`.

```bash
uv run orders-report
#> 2020-01 744     2297
#> 2020-02 696     2154
#> 2020-03 744     2192
#> 2020-04 645     1972
uv run orders-report -p cookie
#> 2020-01 128     376
#> 2020-02 121     371
#> 2020-03 122     340
#> 2020-04 95      278
```
