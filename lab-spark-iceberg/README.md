# Lab: Explore Iceberg tables with Spark

ECE 2026, Big Data course. Spark 4.1.1 (Scala 2.13) with Iceberg 1.11.0, in a PySpark shell on the `vscode-pyspark` Onyxia service.

The lab reuses the Iceberg tables `lab.silver.orders` and `lab.silver.users` written in the Spark DataFrame lab, explores how they are stored on S3, then tests snapshots, time travel and schema evolution. Every section of the lab statement was run in order, followed by the six exercises.

## Adaptations to the environment

- The `default` AWS CLI profile did not exist on the service, so `aws s3 --profile 'default' ...` failed with `The config profile (default) could not be found`. The profile was created once from the Onyxia credentials, so that every `aws` command of the lab could be run unchanged:

  ```bash
  aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID" --profile default
  aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY" --profile default
  aws configure set aws_session_token "$AWS_SESSION_TOKEN" --profile default
  aws configure set endpoint_url "https://$AWS_S3_ENDPOINT" --profile default
  aws configure set region "${AWS_DEFAULT_REGION:-us-east-1}" --profile default
  ```

- The Iceberg tables already existed from the "Write the silver layer as Iceberg tables" section of the previous lab. As stated in the lab, the `createOrReplace()` of "Create an Iceberg table" was therefore skipped, which also kept the snapshot history.
- `orders` already had 2830 rows instead of 5000 in the statement: 2829 silver orders plus one row appended in exercise 4 of the previous lab.
- The `committed_at` timestamps are in UTC (16:09 UTC is 17:09 in Paris).

## Environment

| Check | Result |
|---|---|
| Bucket | `user-c-hu-ece` |
| Spark | 4.1.1 |
| Scala | 2.13.17 |
| `hadoop-aws` in `$SPARK_HOME/jars` | `hadoop-aws-3.4.2.jar` |
| Iceberg runtime | `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0` |

`SHOW NAMESPACES IN lab` returns `silver`, and `SHOW TABLES IN lab.silver` returns `orders` and `users`.

## Create an Iceberg table

```
+------+--------+
|orders|quantity|
+------+--------+
|  2830|    8616|
+------+--------+
```

## Inspect the files

Content of `warehouse/silver/orders/` before any change made in this lab:

| Location | Files | Role |
|---|---|---|
| `data/*.parquet` | 5 (four of 84,055 bytes, one of 1,778 bytes) | The rows. One file per overwrite of the previous lab, plus the one-row append |
| `metadata/*-m0.avro` | 5 | Manifests: list data files with per-file statistics (row count, column bounds) |
| `metadata/snap-<id>-*.avro` | 5 | Manifest lists: one per snapshot, list the manifests of that table version |
| `metadata/vN.metadata.json` | v1 to v5 | Table metadata: schemas, partitioning, list of snapshots, current snapshot |
| `metadata/version-hint.text` | 1 byte | Number of the current metadata version (`5`), read by the `hadoop` catalog |

**Which files contain the actual rows?**
Only the Parquet files of `data/`. The files of `metadata/` contain no business data.

**Which files describe the table?**
The files of `metadata/`. `vN.metadata.json` points to the current snapshot, the snapshot points to its manifest list (`snap-*.avro`), the manifest list points to the manifests (`*-m0.avro`), and the manifests list the Parquet files to read.

**Why does an Iceberg table need more than the Parquet files?**
`data/` holds 5 Parquet files, but the current table uses only 2 of them. The other 3 belong to older snapshots. An engine reading the whole directory would return 4 × 2829 + 1 = 11,317 rows instead of 2830. The metadata says which files make up which version of the table. This is what provides atomic commits, history and time travel, schema evolution, and file skipping based on the statistics of the manifests.

## Explore Iceberg metadata

`lab.silver.orders.snapshots`:

| committed_at (UTC) | snapshot_id | parent_id | operation | added-records | total-records |
|---|---|---|---|---|---|
| 2026-10-28 02:46:54 | 634413565564907819 | NULL | overwrite | 2829 | 2829 |
| 2026-10-28 12:02:19 | 8725273766508709409 | NULL | overwrite | 2829 | 2829 |
| 2026-10-28 12:06:12 | 6505158133339034961 | NULL | overwrite | 2829 | 2829 |
| 2026-10-28 21:42:26 | 2229732332504067350 | NULL | overwrite | 2829 | 2829 |
| 2026-10-28 21:55:35 | 4699836006502291061 | 2229732332504067350 | append | 1 | 2830 |

`lab.silver.orders.files`:

| file_path | file_format | record_count | file_size_in_bytes |
|---|---|---|---|
| `.../data/00003-168-e4afe02b-...parquet` | PARQUET | 1 | 1778 |
| `.../data/00000-60-4c30f099-...parquet` | PARQUET | 2829 | 84055 |

**How many snapshots does the table currently have?**
5: four `overwrite` snapshots, one per `createOrReplace()` of the previous lab, and one `append`.

**How many data files belong to the current table?**
2: the 2829-row file of the last overwrite and the 1-row file of the append. The three other Parquet files are still on S3 but are no longer part of the current version.

**What information does the `snapshots` table contain that the Parquet files do not?**
The history of the table: commit time, snapshot ID, parent snapshot, operation type, path of the manifest list, and a summary of each commit (files and records added, table totals, engine and Iceberg versions, application name). A Parquet file only knows its own content, not which version it belongs to or what changed and when. The four overwrites have a `NULL` parent because each `createOrReplace()` starts a new lineage, while the append has the last overwrite as parent.

## Create a second snapshot

Three existing orders were appended with new `uuid()` order IDs.

```
+-------------------+---------+-----------------------+
|snapshot_id        |operation|committed_at           |
+-------------------+---------+-----------------------+
|634413565564907819 |overwrite|2026-10-28 02:46:54.154|
|8725273766508709409|overwrite|2026-10-28 12:02:19.392|
|6505158133339034961|overwrite|2026-10-28 12:06:12.174|
|2229732332504067350|overwrite|2026-10-28 21:42:26.025|
|4699836006502291061|append   |2026-10-28 21:55:35.417|
|6357918708047721337|append   |2026-11-01 16:09:49.148|
+-------------------+---------+-----------------------+
```

Row count: 2833 (2830 + 3).

**How many snapshots are there now?**
6. The five previous snapshots are kept, and the append added `6357918708047721337`.

**Did the original Parquet files disappear?**
No. An append rewrites nothing. Iceberg wrote one new small Parquet file with the 3 rows (1,973 bytes), one manifest, one manifest list and `v6.metadata.json`. The existing data files stay in place and the new snapshot references them together with the new one (the previous append summary already showed `manifests-kept -> 1`).

**Why does Iceberg create a new snapshot instead of simply replacing the table?**
Files on S3 are never modified, only added. A commit publishes a new metadata file describing the new state. This gives atomicity (a reader sees either the old snapshot or the new one, never a half-written state), isolation (a query started before the commit keeps reading its own snapshot), and history, which enables time travel and rollback.

## Time travel

| Query | count(*) |
|---|---|
| `VERSION AS OF 634413565564907819` (first snapshot) | 2829 |
| Current table | 2833 |

**What does time travel read?**
The table as it was at the requested snapshot. Iceberg finds the snapshot in the table metadata, follows its manifest list and manifests, and reads only the Parquet files that belonged to it. Here it reads only the 2829-row file of 2026-10-28 02:46, without the 4 rows added since.

**Why can Iceberg access an older version without making a complete copy of the table?**
A snapshot holds no data. It is a set of pointers to immutable Parquet files, and consecutive snapshots share most of their files: the last append only added one 3-row file and reused the rest. Keeping an old version only costs its metadata plus the files used by that version alone.

**What would happen if the old snapshot's metadata and data files were physically deleted?**
Time travel to that snapshot would fail with a file-not-found error, since the metadata would reference objects missing from S3. The current table would still work as long as none of its own files were deleted. This is why Iceberg provides maintenance procedures such as `expire_snapshots` and `remove_orphan_files`: they first remove snapshots from the metadata, then delete only the files that no remaining snapshot references. Deleting files by hand on S3 can corrupt the table.

## Schema evolution

After `ALTER TABLE lab.silver.orders ADD COLUMN source STRING`:

```
+----------+---------+-------+
|  col_name|data_type|comment|
+----------+---------+-------+
|  order_id|   string|   NULL|
|   user_id|   string|   NULL|
|ordered_at|timestamp|   NULL|
|  quantity|      int|   NULL|
|   product|   string|   NULL|
|    source|   string|   NULL|
+----------+---------+-------+
```

The `INSERT INTO ... SELECT ..., 'spark' ... LIMIT 1` then added one row with `source = 'spark'`. All older rows read `source` as `NULL`.

**Did the existing Parquet files have to be rewritten?**
No. `ALTER TABLE` only changed the metadata: Iceberg wrote `v7.metadata.json` with the new schema and touched no file of `data/` (no data file, manifest or snapshot was written at 16:14:18 UTC). Old files have no `source` column, and Iceberg returns `NULL` for it when reading them. This works because Iceberg tracks columns by ID, not by name or position. Only the `INSERT` wrote a new Parquet file, which does contain `source`.

**Where is the new schema recorded?**
In the table metadata file, `metadata/vN.metadata.json`. It keeps the list of all successive schemas (`schemas`) and the ID of the current one (`current-schema-id`). Each snapshot also records the schema it was written with, so time travel reads an old version with its old schema.

**Why is schema evolution a table-level operation rather than simply changing a Parquet file?**
A table is made of many files written at different times, and the schema must be the same for the whole table. A Parquet file is immutable: changing its schema means rewriting it, and every file would need to be rewritten at once without disturbing running readers. Keeping the schema in the table metadata makes the change atomic and almost instant, keeps old files readable, and column IDs make renames and drops safe.

## Compare Iceberg with Parquet

The Spark DataFrame lab wrote `silver/spark/orders.parquet/`, which contains one `part-00000-*.snappy.parquet` file and a `_SUCCESS` marker. `warehouse/silver/orders/` contains `data/` and `metadata/` as described above.

**Which one is simply a collection of Parquet files?**
`silver/spark/orders.parquet/`. It is a directory of `part-*.parquet` files plus the `_SUCCESS` marker written by Spark, with no history and no versions. The "table" is whatever files are in the directory at read time.

**Which one has table metadata and snapshots?**
`warehouse/silver/orders/`, through its `metadata/` directory: `vN.metadata.json`, manifest lists `snap-*.avro`, manifests `*-m0.avro` and `version-hint.text`.

**How would you safely replace a Parquet directory while another query is reading it?**
There is no truly safe way. `mode("overwrite")` deletes the old files and then writes new ones, so a running reader can hit missing files or read a mix of old and new files. On S3, renaming a directory is not atomic either: it is a copy then a delete, object by object. The usual workaround is to write to a new versioned directory (for example `orders_v2/`), switch readers to the new path, and delete the old directory only once nobody reads it anymore. This rebuilds by hand what Iceberg does.

**How does Iceberg make the table state visible atomically?**
It first writes all new files (data, manifests, manifest list, `vN+1.metadata.json`) without touching the old ones, so no reader sees them yet. The commit then swaps a single pointer to the current metadata file. With the `hadoop` catalog of this lab, the pointer is `version-hint.text`. With a REST, Hive or Nessie catalog, it is an atomic compare-and-swap on the server. A reader follows either the old or the new pointer and always sees a complete state.

**Why is a catalog needed once several engines need to share the same Iceberg table?**
All engines (Spark, Trino, DuckDB, Flink...) must agree on the current metadata file, otherwise each one would see a different version of the table. With concurrent writers, a single arbiter must also accept one commit and reject the other, so that the loser retries on the new version instead of overwriting the first one. The `hadoop` catalog relies on an atomic rename of the file system, which S3 does not guarantee, so it only fits a single writer as in this lab. A catalog also resolves tables by name (`lab.silver.orders`) instead of S3 paths, and handles access control.

## Exercises

### 1. Create `lab.silver.users` as an Iceberg table

The table was already created in the previous lab with the same code as in this lab:

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")

stg_users.writeTo(
    "lab.silver.users"
).createOrReplace()
```

As for `orders`, it was not recreated. It was checked with the same queries:

| Check | Result |
|---|---|
| `count(*)` | 50 |
| Snapshots | 4, all `overwrite` (2026-10-28 02:46, 12:02, 12:06, 21:42 UTC) |
| Columns | `user_id`, `username`, `name`, `sex`, `email`, `birthdate` (date), `street`, `city`, `state`, `zip_code` |

### 2. Append a new batch of users and inspect the snapshots

Same method as "Create a second snapshot":

```python
stg_users = spark.table("lab.silver.users")
new_users = stg_users.limit(3).withColumn("user_id", F.expr("uuid()"))
new_users.writeTo("lab.silver.users").append()
```

```
+-------------------+---------+-----------------------+
|snapshot_id        |operation|committed_at           |
+-------------------+---------+-----------------------+
|8703751544854962294|overwrite|2026-10-28 02:46:51.657|
|4736411982691285814|overwrite|2026-10-28 12:02:17.237|
|1715217136567636190|overwrite|2026-10-28 12:06:10.039|
|8732031205681896068|overwrite|2026-10-28 21:42:24.114|
|7402095646126257560|append   |2026-11-01 16:23:04.173|
+-------------------+---------+-----------------------+
```

The append created a 5th snapshot and the table went from 50 to 53 users. The 50-row file of the last overwrite was kept, and only a new 3-row file was added.

### 3. Query the table before and after the append using time travel

The "before" version is the last overwrite (`snapshots[-2]`), the "after" version is the append (`snapshots[-1]`).

| Query | count(*) |
|---|---|
| `VERSION AS OF 8732031205681896068` (before the append) | 50 |
| `VERSION AS OF 7402095646126257560` (after the append) | 53 |
| Current table | 53 |

The current table matches the last snapshot, since it is the current version.

### 4. Add a `country` column and write new data using the evolved schema

```python
spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")

spark.sql("""
    INSERT INTO lab.silver.users
    SELECT user_id, username, name, sex, email, birthdate,
           street, city, state, zip_code, 'US'
    FROM lab.silver.users
    LIMIT 1
""")
```

`DESCRIBE` shows `country` (`string`) as the 11th column. `'US'` was used because `state` holds US state codes.

```
+-------+-----+
|country|users|
+-------+-----+
|   NULL|   53|
|     US|    1|
+-------+-----+
```

The 53 existing rows come from files written before the change and read `country` as `NULL`. They were not rewritten. Only the inserted row, written with the evolved schema, has a value.

### 5. Compare the S3 layout of the users and orders tables

| | `users` | `orders` |
|---|---|---|
| Parquet files in `data/` | 6 | 7 |
| Size of a full overwrite file | 7,435 bytes (50 rows) | 84,055 bytes (2829 rows) |
| Snapshots (`snap-*.avro`) | 6 | 7 |
| Manifests (`*-m0.avro`) | 6 | 7 |
| `metadata.json` files | v1 to v7 | v1 to v8 |
| `version-hint.text` | 7 | 8 |
| Added column | `country` (v6) | `source` (v7) |

Changes made today, read from the timestamps (UTC):

| Table | Time | Operation | Files written |
|---|---|---|---|
| `orders` | 16:09:48 | append of 3 orders | Parquet (1,973 bytes), manifest, snapshot `6357...`, `v6.metadata.json` |
| `orders` | 16:14:18 | `ADD COLUMN source` | `v7.metadata.json` only |
| `orders` | 16:14:29 | insert with `source` | Parquet (2,027 bytes), manifest, snapshot `8534...`, `v8.metadata.json` |
| `users` | 16:23:03 | append of 3 users | Parquet (3,312 bytes), manifest, snapshot `7402...`, `v5.metadata.json` |
| `users` | 16:24:24 | `ADD COLUMN country` | `v6.metadata.json` only |
| `users` | 16:24:32 | insert with `country` | Parquet (3,553 bytes), manifest, snapshot `6657...`, `v7.metadata.json` |

Both tables have the same structure: `data/` for the Parquet files, `metadata/` with the three levels (table metadata, manifest lists, manifests), and `version-hint.text` for the `hadoop` catalog. The only differences are the size of the data and the history: `orders` has one more snapshot because of the one-row append of the previous lab. In both tables:

- each write (overwrite, append, insert) adds one Parquet file, one manifest, one manifest list and one `metadata.json`
- an `ALTER TABLE` only adds one `metadata.json`, with no snapshot
- no file is ever modified or deleted. The four large files of the old overwrites are still on S3 although the current table only uses the last one. Expiring old snapshots is the only way to free that space.

The `users` file of the `country` insert is slightly larger than the 3-row append file although it has a single row, because it holds one more column.

### 6. Parquet, Iceberg and an Iceberg catalog

**Parquet** is a file format. It stores data by column, compressed, with a schema and statistics inside each file. A file only knows itself: a directory of Parquet files such as `silver/spark/orders.parquet/` is just a set of files, with no notion of version, commit or shared schema.

**Iceberg** is a table format. It adds a metadata layer on top of the Parquet files (`metadata.json`, manifest lists, manifests) which states exactly which files make up the table at each version, with which schema and partitioning. This gives atomic commits, snapshots, time travel, schema evolution without rewriting data (exercise 4), and file skipping based on manifest statistics.

**An Iceberg catalog** maps a table name (`lab.silver.orders`) to the location of its current metadata file. Committing means moving that pointer atomically. In this lab, the `lab` catalog is of type `hadoop`: the pointer is simply `version-hint.text` in the bucket, which is enough for a single writer but not safe on S3 with several writers. When several engines share a table, a real catalog (REST, Hive Metastore, Nessie, Glue) acts as the single arbiter: everyone reads the same current version, and two concurrent commits cannot overwrite each other.

In short, Parquet stores the data, Iceberg describes the table and its versions, and the catalog says which version is the current one.

## Cleanup

The lab ends by removing the Iceberg warehouse and stopping Spark:

```bash
aws s3 --profile default rm \
  "s3://$LAB_BUCKET_NAME/warehouse/" --recursive
```

```python
spark.stop()
```

These commands are run after this README is pushed. They delete the `orders` and `users` tables of the previous lab as well, so the Iceberg metadata committed in `warehouse/` for that lab no longer has matching data on S3.