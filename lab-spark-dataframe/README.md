# Lab: Bronze to silver with Spark DataFrames

ECE 2026, Big Data course. Spark 4.1 with Iceberg 1.11, in a PySpark shell on the `vscode-pyspark` Onyxia service.

The lab reads the bronze `users.csv` and `orders.csv` datasets from S3, builds the same silver layer as the dbt lab with the DataFrame API and with Spark SQL, writes it to S3 as Parquet and as Iceberg tables, and compares the result with the dbt output. Every section of the lab statement was run in order, followed by the four exercises.

## Adaptations to the environment

- The `default` AWS CLI profile does not exist on the service. The `aws` commands were run without `--profile`, with `AWS_ENDPOINT_URL="https://$AWS_S3_ENDPOINT"` exported instead.
- The dbt outputs are named `silver/dbt/orders.parquet` and `silver/dbt/users.parquet` in this project, not `stg_orders.parquet` and `stg_users.parquet`. The DuckDB queries use these names.
- The Spark UI is not reachable through the Onyxia proxy. The Storage tab of exercise 3 was read from the Spark UI REST API (`/api/v1/applications/<app-id>/storage/rdd`).
- `%%timeit` only exists in Jupyter. Exercise 2 is timed with the `timeit` module of Python in the PySpark shell.

## Results

### Bronze layer

| Dataset | Rows |
|---|---|
| `users.csv` | 50 |
| `orders.csv` | 2829 |

With `multiLine=True`, the address is read as one field on two lines, for example `908 Jennifer Squares\nRobinsonshire, KY 01352`.

### Silver layer

| Step | Orders |
|---|---|
| Original | 2829 |
| With 3 injected duplicates | 2832 |
| After window deduplication | 2829 |

The DataFrame and Spark SQL versions of the deduplication pass both `assert` checks: same row count, same columns.

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
silver/dbt/orders.parquet                                                         72466 B
silver/dbt/users.parquet                                                           7018 B
silver/spark/orders.parquet/_SUCCESS                                              11799 B
silver/spark/orders.parquet/part-00000-0c13add7-...-c000.snappy.parquet          139556 B
silver/spark/users.parquet/_SUCCESS                                               11783 B
silver/spark/users.parquet/part-00000-10cd7492-...-c000.snappy.parquet             9581 B
```

Read back: 50 users and 2829 orders, both `assert` checks pass.

### Iceberg tables

The tables already existed from an earlier attempt, so `createOrReplace()` added a fourth snapshot instead of starting over:

| snapshot_id | operation | committed_at |
|---|---|---|
| 634413565564907819 | overwrite | 2026-10-28 02:46:54 |
| 8725273766508709409 | overwrite | 2026-10-28 12:02:19 |
| 6505158133339034961 | overwrite | 2026-10-28 12:06:12 |
| 2229732332504067350 | overwrite | 2026-10-28 21:42:26 |

The current snapshot of `lab.silver.orders` has one data file: Parquet, 2829 records, 84 055 bytes. Read back through the catalog: 50 users and 2829 orders, both `assert` checks pass.

Content of `warehouse/silver/users/metadata/` (same structure for `orders`):

```
v1.metadata.json ... v4.metadata.json      one per table version
snap-<snapshot_id>-1-<uuid>.avro            manifest list, one per snapshot
<uuid>-m0.avro                              manifest, one per snapshot
version-hint.text                           current version (4)
```

The metadata directories of both tables were committed with `feat(spark-lab): add Iceberg table metadata`. The `data/` directories were not committed.

### Comparison with the dbt lab

| engine | orders | quantity | users |
|---|---|---|---|
| dbt | 2829 | 8615 | 50 |
| spark | 2829 | 8615 | 50 |

Schemas of the orders model:

| Column | dbt | Spark |
|---|---|---|
| `order_id` | `uuid` | `varchar` |
| `user_id` | `uuid` | `varchar` |
| `ordered_at` | `timestamp with time zone` | `timestamp` |
| `quantity` | `integer` | `integer` |
| `product` | `varchar` | `varchar` |

## Answers to the questions

### SparkSession with S3 access

**The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?**

The job is lost. In `local[*]` mode the driver and the executors run in the JVM of the service container, so a restart kills all of them, along with the cached data and every variable of the shell. The new container receives new STS credentials, and the session has to be built again with them, because S3A only read the credentials when the session started. Even without a restart, a long job fails with a 403 error once the token expires, since nothing refreshes it. This happened during the lab: each time the container was killed, every DataFrame had to be rebuilt from the bronze files.

**Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?**

It only reads the access key and the secret key and ignores the session token. Temporary STS keys are only valid together with their token, so S3 rejects the signed requests. Long-lived keys, such as those of an IAM user or a MinIO account, have no token, which is why the simple provider works with them.

### Silver layer

**Explain the output of the code that was just run.**

The query counts the silver users per state, and how many of them have a NULL city. It is a check of the address parsing. `AE` and `AP` are US military "states" used by APO, FPO and DPO addresses such as `DPO AP 09617`, which have no city: all their users have a NULL city, as expected. Every other state has a city for each of its users, so the regular expression matched them. Without the conversion of empty strings to NULL, `missing_city` would be 0 everywhere and the military addresses would be counted under an empty city.

**Compare the DataFrame and SQL cells. Which one for a one-off exploration, which one for a model reused by several colleagues? Why?**

For a one-off exploration, SQL: it is quick to write, everybody on a data team can read it, and the query can be pasted into another tool. For a model reused by several colleagues, the DataFrame API: the logic can be split into functions, parameterised and unit tested, and the editor checks the Python code, whereas a SQL string is only checked when it runs. Both compile to the same Catalyst plan, so performance does not decide.

**Where else could the empty string of `regexp_extract()` silently produce a wrong result?**

In `state` and `zip_code`, which are extracted with the same function and left as they are. An address whose second line does not match the pattern gets `""` in both columns instead of NULL. A `groupBy("state")` then shows an empty state as a real group, and `count(state)` or `isNull()` checks miss the missing values. The UDFs of exercise 2 return `""` for the same reason, to match.

### Write the silver layer as Parquet

**List the objects written under `silver/users.parquet/`. How many part-files are there, and why?**

One part-file, `part-00000-...-c000.snappy.parquet`, plus a `_SUCCESS` marker written by the committer. Spark writes one file per partition of the DataFrame. The users come from a 7 KB CSV, read as a single partition, so there is one file. The orders go through the shuffle of the window function, but adaptive query execution merges the small shuffle partitions into one before the write, so they also end up in a single file.

**`coalesce(1)` before `.write` would produce a single file. What do you lose on a larger dataset?**

The parallelism. A single task writes all the data, on one core of one executor, which is slow and can run out of memory or disk. Since `coalesce()` does not shuffle, the single partition can also be pushed up into the previous stages, so the whole computation runs on one core. Readers then get one large file instead of several files they can read in parallel.

### Write the silver layer as Iceberg tables

**Compare `silver/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the table?**

In `silver/spark/orders.parquet/`, every file is data. `_SUCCESS` is only a marker, and the schema is repeated in the footer of each Parquet file. In `warehouse/silver/orders/`, the data is in the Parquet files of `data/`, and the files of `metadata/` describe the table. Each `vN.metadata.json` holds the schema, the partition spec and the list of snapshots. The `snap-*.avro` manifest lists point to the manifests of one snapshot. The `*-m0.avro` manifests list the data files with their statistics. `version-hint.text` gives the current version.

**A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes with an Iceberg table?**

With plain Parquet, the overwrite deletes the old files and then writes the new ones. The reader can fail on a file that disappeared, read an empty directory, or combine old and new files and return a wrong result. With Iceberg, the writer adds new data files, then switches the pointer to the new metadata in one step. The reader keeps the snapshot it resolved when it started, so it sees either the old version or the new one, complete. The old data files stay in place until the snapshots are expired: the bucket still holds one data file per snapshot.

**With this catalog, the pointer to the current version is a file in the bucket. What can go wrong if two jobs commit at the same time, and what would a catalog service add?**

A commit writes `v(N+1).metadata.json` and updates `version-hint.text`. S3 has no atomic rename, so two jobs can both write version N+1: one commit silently replaces the other and its changes are lost, or the hint points to the wrong file. A catalog service, such as an Iceberg REST catalog, Nessie, AWS Glue or a Hive Metastore, holds the pointer to the current metadata and updates it with an atomic compare-and-swap. One job wins and the other gets a conflict it can retry. The service also gives every engine one place to find the tables and to control access.

### Compare with the dbt lab

**What differs between the two schemas? Spark has no `UUID` type: what did you do about it, and what is the consequence for the consumers of the silver layer?**

`order_id` and `user_id` are `uuid` with dbt and `varchar` with Spark, and `ordered_at` is `timestamp with time zone` with dbt and `timestamp` with Spark. Since Spark has no UUID type, the identifiers were cast to strings. Consumers get 36-character strings instead of 16-byte values, with no check of the format, and a join between the Spark output and the dbt output needs a cast on one side. For the dates, Spark stores the instant in UTC, but the Parquet file carries no time zone, so readers have to know that the values are UTC.

**One object per model with dbt, one directory of part-files with Spark. Which one is easier for a consumer to read? Which one scales?**

The single object of dbt is easier to consume: one path, readable by any tool without a glob. The directory of part-files is what scales: each task writes its own file in parallel, and readers can split their work across the files. A table format such as Iceberg keeps the scaling and gives consumers a single table name again.

**The dbt lab uses the `unique`, `not_null`, `accepted_values` and `relationships` tests. What did this lab use instead, and what would you need to write to get the same guarantees?**

`assert` statements on row counts and column sets, and a `groupBy(...).count()` filtered on `count > 1` to look for duplicates. They only run when someone runs the cells, and they only cover counts. The same guarantees need one check per test that stops the job when it fails: `unique` as a `groupBy` on the key with no count above 1, `not_null` as a count of `isNull()` rows equal to 0, `accepted_values` as a count of rows outside an `isin()` list equal to 0, and `relationships` as a `left_anti` join from orders to users returning no row. Libraries such as Great Expectations, Soda or Deequ package these checks.

**`ref()` and `source()` give dbt the dependency graph of the models. What plays this role in the notebook?**

The Python variables and the order in which the cells are run. `stg_orders` is built from `orders_with_duplicates`, itself built from `orders_typed`, and Spark turns these references into a lazy plan. That graph only exists inside one session: nothing documents the lineage across scripts or rebuilds the models that depend on a change. The bronze paths and the catalog table names such as `lab.silver.orders` play the role of `source()`.

**Why are the build times of `dbt build` and of the cells of this lab not directly comparable?**

The Spark time includes the start of the JVM, the download of the Iceberg jar and the creation of the session, which dominate on 3000 rows. The cells are run by hand with pauses between them, every action reads the CSV files from S3 again because nothing is cached, and several actions are only there as checks. `dbt build` runs DuckDB in-process, an engine designed for small data on one machine, and it also runs the tests. The two runs also happened at different times on a shared service with a different load.

**dbt with DuckDB or Spark: a daily batch of 10 GB, a daily batch of 10 TB, a team of analysts who only know SQL?**

A daily batch of 10 GB: dbt with DuckDB, which handles it on one machine with much less to operate. A daily batch of 10 TB: Spark on a cluster, because a single machine cannot process it in time. A team of analysts who only know SQL: dbt, since models, tests and documentation are all written in SQL. If the volume grows, dbt can run on Spark or on a warehouse through an adapter.

## Exercises

### 1. `birthdate_is_valid`

The first order date of each user is computed with `min(ordered_at)` and joined to `stg_users` with a left join. The column is `False` when the birthdate is later than that date, and `True` for users without any order. The DataFrame and Spark SQL versions give the same result, checked with `exceptAll()` in both directions: 0 differences.

| birthdate_is_valid | users |
|---|---|
| true | 45 |
| false | 5 |

The 5 invalid users were born between 2022 and 2026, after orders placed in 2020.

The dbt lab handles the same rule with a singular test, `tests/assert_orders_after_user_birth.sql`, which returns every order placed before the birthdate of its user (`where o.ordered_at < u.birthdate`) and fails the build when it returns rows. The dbt test leaves the data unchanged and reports one row per faulty order. The Spark version keeps every row and flags each user, so consumers can filter them. Both rules point to the same users: if one order is placed before the birthdate, so is the first one.

### 2. Python UDF and `pandas_udf`

The zip code extraction was rewritten as a plain Python UDF and as a `pandas_udf`. Both return the same values as `regexp_extract()` on all rows (0 mismatches). Each run writes to the `noop` sink, so every row is computed. Timings over 5 runs:

| Version | Bronze not cached, best / mean | Bronze cached, best / mean |
|---|---|---|
| `regexp_extract()` | 0.117 s / 0.126 s | 0.053 s / 0.065 s |
| Python UDF | 0.111 s / 0.128 s | 0.104 s / 0.212 s |
| `pandas_udf` | 0.113 s / 0.135 s | 0.103 s / 0.303 s |

Without the cache, the three versions take the same time: on 50 rows, the job is dominated by the scheduling and by the read of `users.csv` from S3. Once the input is cached, `regexp_extract()` becomes twice as fast because it runs in the JVM, while both UDFs stay around 0.1 s because their cost now comes from sending the data to a Python worker and back. On a large dataset, the expected order is `regexp_extract()`, then `pandas_udf`, which moves data in Arrow batches, then the plain UDF, which serialises every row.

### 3. Cache

| | `users_bronze.count()` | `orders_bronze.count()` |
|---|---|---|
| Before `.cache()` | 0.364 s | 0.313 s |
| After `.cache()` | 0.115 s | 0.121 s |

Storage tab after the cache:

| Cached plan | Partitions | Memory |
|---|---|---|
| `FileScan csv` of `users.csv` | 1 / 1 | 9 416 B |
| `FileScan csv` of `orders.csv` | 1 / 1 | 147 856 B |

Before the cache, every action reads the CSV files from S3 again. After the first action fills the cache, the later stages read the partitions from memory, which makes the counts about three times faster and halves the `regexp_extract()` benchmark of exercise 2.

### 4. Append and time travel

One order was appended with `writeTo("lab.silver.orders").append()`. The table now has a fifth snapshot:

| snapshot_id | operation | committed_at |
|---|---|---|
| 634413565564907819 | overwrite | 2026-10-28 02:46:54 |
| 8725273766508709409 | overwrite | 2026-10-28 12:02:19 |
| 6505158133339034961 | overwrite | 2026-10-28 12:06:12 |
| 2229732332504067350 | overwrite | 2026-10-28 21:42:26 |
| 4699836006502291061 | append | 2026-10-28 21:55:35 |

| Read | Rows |
|---|---|
| `lab.silver.orders`, current | 2830 |
| `lab.silver.orders VERSION AS OF 2229732332504067350` | 2829 |
| Parquet copy before the append | 2829 |
| Parquet copy after the append | 2830 |

The same append was done on a copy of the Parquet output (`silver/spark/orders_ex4.parquet`), so the directory used for the dbt comparison stays unchanged. With Iceberg, the previous version is still readable with `VERSION AS OF`, because each snapshot is recorded in the metadata and its data files are kept. With Parquet, the append only adds another part-file to the directory: nothing records the previous state, so there is no way to read the table as it was before.
