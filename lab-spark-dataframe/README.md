# Lab: Bronze to silver with Spark DataFrames

ECE 2026, Big Data course. Spark 4.1 with Iceberg 1.11 on the `vscode-pyspark` Onyxia service.

The lab reads the bronze `users.csv` and `orders.csv` datasets from S3, builds the same silver layer as the dbt lab with the DataFrame API and with Spark SQL, writes it to S3 as Parquet and as Iceberg tables, and compares the result with the dbt output.

## Status

| Part | Status |
|---|---|
| SparkSession with S3 and Iceberg | done |
| Bronze layer | done |
| Silver layer, DataFrame API and Spark SQL | done |
| Parquet output | done |
| Iceberg tables | done |
| Comparison with the dbt lab | done |
| Exercises 1 to 4 | code written in `lab_silver.py`, not executed |
| Commit of the Iceberg metadata | not done |

The service has a 2 GB memory limit while the lab asks for 4 GB. The container was killed several times during the run. Recreating the service with 4 GB was then refused by the namespace quota (`exceeded quota: onyxia-quota`), even with no service listed. The exercises and the metadata commit could not be run for that reason. All results below come from real runs.

## Content

| File | Description |
|---|---|
| `lab_silver.py` | The whole lab and the four exercises in one script |
| `README.md` | Results and answers to the lab questions |

## How to run

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  lab-spark-dataframe/lab_silver.py
```

The session uses `local[4]` and 8 shuffle partitions instead of `local[*]`. Onyxia exposes 16 cores to the container, and one Python worker per core went beyond the memory limit.

Spark outputs are written under `silver/spark/` and the dbt outputs are under `silver/dbt/` (`orders.parquet`, `users.parquet`), so the paths differ slightly from the lab statement.

## Results

### Bronze layer

| Dataset | Rows |
|---|---|
| `users.csv` | 50 |
| `orders.csv` | 2829 |

The `address` column is read correctly on two lines thanks to `multiLine=True`, for example `908 Jennifer Squares\nRobinsonshire, KY 01352`.

### Silver layer

| Step | Orders |
|---|---|
| Original | 2829 |
| With 3 injected duplicates | 2832 |
| After window deduplication | 2829 |

The DataFrame and Spark SQL versions return the same number of rows and the same columns (both `assert` pass).

Users per state, first 10 rows:

| state | users | missing_city |
|---|---|---|
| AE | 3 | 3 |
| FM | 3 | 0 |
| NJ | 2 | 0 |
| VT | 2 | 0 |
| DE | 2 | 0 |
| VI | 2 | 0 |
| NM | 2 | 0 |
| PA | 2 | 0 |
| TX | 2 | 0 |
| AP | 2 | 2 |

### Parquet output

```
silver/spark/orders.parquet/_SUCCESS
silver/spark/orders.parquet/part-00000-38eef914-...-c000.snappy.parquet   139556 B
silver/spark/users.parquet/_SUCCESS
silver/spark/users.parquet/part-00000-62801124-...-c000.snappy.parquet      9581 B
```

Read back: 50 users and 2829 orders, no row lost or duplicated.

### Iceberg tables

The script was run three times, so each table has three snapshots:

| snapshot_id | operation | committed_at |
|---|---|---|
| 634413565564907819 | overwrite | 2026-10-28 02:46:54 |
| 8725273766508709409 | overwrite | 2026-10-28 12:02:19 |
| 6505158133339034961 | overwrite | 2026-10-28 12:06:12 |

Data files of the current snapshot of `lab.silver.orders`: one Parquet file, 2829 records, 84 055 bytes.

Layout of `warehouse/silver/orders/`:

```
data/00000-68-4070e51e-...-00001.parquet        84055 B   (snapshot 1)
data/00000-68-346db84b-...-00001.parquet        84055 B   (snapshot 2)
data/00000-68-ec81f6ea-...-00001.parquet        84055 B   (snapshot 3)
metadata/v1.metadata.json
metadata/v2.metadata.json
metadata/v3.metadata.json
metadata/version-hint.text
metadata/snap-634413565564907819-1-....avro    (manifest list, one per snapshot)
metadata/snap-8725273766508709409-1-....avro
metadata/snap-6505158133339034961-1-....avro
metadata/e3f04662-...-m0.avro                    (manifest, one per snapshot)
metadata/6a96779d-...-m0.avro
metadata/e4c4c38c-...-m0.avro
```

`warehouse/silver/users/` has the same structure. Read back through the catalog: 50 users and 2829 orders.

### Comparison with the dbt lab

Computed with the DuckDB CLI on both outputs:

| engine | orders | quantity | users |
|---|---|---|---|
| dbt | 2829 | 8615 | 50 |
| spark | 2829 | 8615 | 50 |

Schemas of the orders model:

| Column | dbt (DuckDB) | Spark |
|---|---|---|
| `order_id` | `uuid` | `varchar` |
| `user_id` | `uuid` | `varchar` |
| `ordered_at` | `timestamp with time zone` | `timestamp` |
| `quantity` | `integer` | `integer` |
| `product` | `varchar` | `varchar` |

## Answers to the questions

### SparkSession with S3 access

**What happens to a running Spark job if the service is restarted mid-lab?**
The job is lost. In `local[*]` mode the driver and the executors live in the JVM of the service container, so a restart kills all of them, along with the cached data and the shell state. The new container gets new STS credentials, and the session has to be built again with them, because S3A only read the credentials when the session started. Even without a restart, a long job fails with a 403 error as soon as the token expires, since nothing refreshes it. This happened several times during the lab: each time the container was killed, every DataFrame had to be rebuilt from the bronze files.

**Why does a plain `SimpleAWSCredentialsProvider` fail here?**
It only reads the access key and the secret key and ignores the session token. Temporary STS keys are only valid together with their token, so S3 rejects the requests. It works with long-lived keys (an IAM user or a MinIO account), which have no token.

### Silver layer

**Explain the output of the code that was just run.**
The query counts the silver users per state, and how many of them have a NULL city. It checks the address parsing. `AE` and `AP` are US military "states" (APO, FPO and DPO addresses such as `DPO AP 09617`), which have no city: all their users have a NULL city, as expected. Every other state has a city for each user, so the regular expression matched them. Without the conversion of empty strings to NULL, `missing_city` would be 0 everywhere and the military addresses would be counted under an empty city.

**DataFrame or SQL, for a one-off exploration and for a shared model?**
For a one-off exploration, SQL: it is quick to write, everybody can read it, and it is easy to paste into another tool. For a model reused by several colleagues, the DataFrame API: the logic can be split into functions, parameterised, unit tested, and the editor checks the Python code, while a SQL string is only checked when it runs. Both compile to the same Catalyst plan, so performance does not decide.

**Where else could the empty string of `regexp_extract()` give a wrong result?**
In `state` and `zip_code`, extracted with the same function and left as they are. An address whose second line does not match the pattern gets `""` instead of NULL in both columns. A `groupBy("state")` then shows an empty state as a real group, and `count(state)` or `isNull()` checks do not see the missing values. The UDFs of exercise 2 return `""` for the same reason.

### Write the silver layer as Parquet

**How many part-files are there, and why?**
One part-file per output, plus a `_SUCCESS` marker. Spark writes one file per partition of the DataFrame. The users come from a 7 KB CSV, which is read as a single partition. The orders go through the window shuffle, but adaptive query execution merges the small shuffle partitions into one before the write, so they also end up in one file.

**What do you lose with `coalesce(1)` on a larger dataset?**
The parallelism. One task writes all the data, on one core and one executor, which is slow and can run out of memory or disk. Since `coalesce` does not shuffle, it can also push the single partition up into the previous stages, so the whole computation runs on one core. Readers then get one big file instead of files they can read in parallel.

### Write the silver layer as Iceberg tables

**Which files hold the data, and which ones describe the table?**
In `silver/spark/orders.parquet/`, every file is data. The `_SUCCESS` file is only a marker, and the schema is repeated in the footer of each Parquet file. In `warehouse/silver/orders/`, the data is in the Parquet files of `data/`. The files of `metadata/` describe the table: each `vN.metadata.json` holds the schema, the partition spec and the list of snapshots, the `snap-*.avro` manifest lists point to the manifests of one snapshot, the `*-m0.avro` manifests list the data files with their statistics, and `version-hint.text` gives the current version (3 here).

**A second job overwrites the Parquet directory while a query reads it. What does the reader see? What changes with Iceberg?**
With plain Parquet, the overwrite deletes the old files and then writes the new ones. The reader can fail on a file that disappeared, read an empty directory, or mix old and new files, which gives a wrong result. With Iceberg, the writer adds new data files and then switches the pointer to the new metadata in one step. The reader keeps the snapshot it resolved when it started, so it sees either the old version or the new one, complete. The listing above shows it: the data files of the three snapshots are all still there.

**What can go wrong if two jobs commit at the same time with this catalog?**
A commit writes `v(N+1).metadata.json` and updates `version-hint.text`. On S3 there is no atomic rename, so two jobs can both write version N+1. One commit silently replaces the other and its data is lost, or the hint ends up pointing to the wrong file. A catalog service (Iceberg REST catalog, Nessie, Glue, Hive Metastore) holds the pointer to the current metadata and updates it with an atomic compare-and-swap: one job wins and the other gets a conflict it can retry. It also gives one place to find the tables and control access, for every engine.

### Compare with the dbt lab

**What differs between the two schemas?**
`order_id` and `user_id` are `uuid` with dbt and `varchar` with Spark, and `ordered_at` is `timestamp with time zone` with dbt and `timestamp` with Spark. Spark has no UUID type, so the lab casts the identifiers to strings. Consumers get 36-character strings instead of 16-byte values, with no check of the format, and joining the Spark output with the dbt output needs a cast on one side. For the dates, Spark stores the instant in UTC but the Parquet file carries no time zone, so readers have to know it is UTC.

**One object per model or one directory of part-files?**
The single object of dbt is easier for a consumer: one path, readable by any tool without a glob. The directory of part-files is what scales: each task writes its own file in parallel, and readers can split the work across files. A table format such as Iceberg keeps the scaling and gives consumers one table name again.

**What did this lab use instead of the dbt tests?**
`assert` statements on row counts and column sets, and a `groupBy(...).count()` filtered on `count > 1` to look for duplicates. They run only when the notebook runs and they cover only the counts. Getting the same guarantees as dbt means writing one check per test and stopping the job when it fails: `unique` as a `groupBy` on the key with no count above 1, `not_null` as a count of `isNull()` rows equal to 0, `accepted_values` as a count of rows outside an `isin()` list equal to 0, and `relationships` as a `left_anti` join from orders to users returning no row. A library such as Great Expectations, Soda or Deequ packages these checks.

**What plays the role of `ref()` and `source()`?**
The Python variables and the order of the cells. `stg_orders` is built from `orders_with_duplicates`, itself built from `orders_typed`, and Spark turns these references into a lazy plan. That only exists inside one session: nothing gives a graph of the models across scripts, rebuilds what depends on a change, or documents the lineage. The bronze paths and the catalog table names (`lab.silver.orders`) play the role of `source()`.

**Why are the build times of dbt and Spark not directly comparable?**
The Spark time includes the start of the JVM, the download of the Iceberg jar and the creation of the session, which dominate on 3000 rows. The cells are run by hand with pauses, every action reads the CSV files from S3 again because nothing is cached, and several actions are only there for checks. `dbt build` runs DuckDB in-process, which is designed for small data on one machine, and it also runs the tests. The two also ran at different times on a shared service with a different load.

**dbt with DuckDB or Spark?**
A daily batch of 10 GB: dbt with DuckDB, which handles it on one machine with less to operate. A daily batch of 10 TB: Spark, on a cluster, because one machine can no longer process it in time. A team of analysts who only know SQL: dbt, since the models, tests and documentation are all SQL. If the volume grows, dbt can run on Spark or on a warehouse through an adapter.

## Exercises

The code of the four exercises is in `lab_silver.py`. It could not be run because of the memory and quota issue described above, so no output is reported here.

**1. `birthdate_is_valid`.** The first order date of each user is computed with `min(ordered_at)`, then joined to `stg_users`. The column is `False` when the birthdate is later than that date, and `True` for users with no order. It is written once with the DataFrame API and once in SQL, and `exceptAll()` in both directions checks that the two give the same values.

**2. UDFs.** The zip code is extracted with a plain Python UDF and with a `pandas_udf`, and both are timed against `regexp_extract()` with `time.perf_counter()` (the script is not a notebook, so `%%timeit` is not available). Each run writes to the `noop` sink so every row is computed. The expected order is `regexp_extract()` fastest, as it runs in the JVM, then `pandas_udf`, which sends data to Python in Arrow batches, then the plain UDF, which pays the serialization cost row by row.

**3. Cache.** `users_bronze` and `orders_bronze` are counted before and after `.cache()`. The Spark UI is not reachable through the Onyxia proxy, so the Storage tab is read through the Spark UI REST API (`/api/v1/applications/<app-id>/storage/rdd`).

**4. Time travel.** One order is appended with `writeTo("lab.silver.orders").append()`, which adds a snapshot with the `append` operation. `VERSION AS OF <previous snapshot id>` reads the table as it was before the append. The same append is done on a copy of the Parquet output (`orders_ex4.parquet`): the new rows are added as another part-file and there is no way to read the previous state, because a Parquet directory keeps no history.
