"""Lab: Bronze to silver with Spark DataFrames.
 
Run from the repository root, in the vscode-pyspark Onyxia service:
 
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
        lab-spark-dataframe/lab_silver.py
 
The bronze files must already be in s3://$KUBERNETES_NAMESPACE/bronze/.
"""
 
import json
import os
import re
import time
import urllib.request
from datetime import datetime
 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
 
os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
 
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
 
 
def section(title):
    print(f"\n===== {title} =====")
 
 
# ---------------------------------------------------------------------------
section("SparkSession with S3 access")
 
spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    # local[*] starts one Python worker per visible core (16 on Onyxia), which
    # exceeded the 2 GB memory limit of the service. 4 cores and 8 shuffle
    # partitions are plenty for a few thousand rows.
    .master("local[4]")
    .config("spark.sql.shuffle.partitions", "8")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
print(spark)
 
# ---------------------------------------------------------------------------
section("Bronze layer")
 
users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv",
    header=True, inferSchema=True, samplingRatio=0.01, multiLine=True,
)
orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv",
    header=True, inferSchema=True, samplingRatio=0.01, multiLine=True,
)
 
users_bronze.printSchema()
users_bronze.show(3, truncate=False)
users_bronze.select("address").show(3, truncate=False)
print("users:", users_bronze.count(), "orders:", orders_bronze.count())
 
# ---------------------------------------------------------------------------
section("Silver users, DataFrame API")
 
users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)
 
stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)
 
# regexp_extract() returns "" instead of NULL when nothing matches.
stg_users = stg_users.withColumn(
    "city", F.when(F.col("city") == "", None).otherwise(F.col("city"))
)
stg_users.show(5, truncate=False)
 
# ---------------------------------------------------------------------------
section("Silver orders, window deduplication")
 
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())
 
# Intentional duplicates, to check that the deduplication works
duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show()
 
dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()
 
# ---------------------------------------------------------------------------
section("Same transformation with Spark SQL")
 
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql("""
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *,
               row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
""")
stg_orders_sql.show(5)
 
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("DataFrame and SQL versions agree")
 
stg_users.createOrReplaceTempView("stg_users")
spark.sql("""
    SELECT state,
           count(*) AS users,
           sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users
    GROUP BY state
    ORDER BY users DESC
""").show(10)
 
# ---------------------------------------------------------------------------
section("Write the silver layer as Parquet")
 
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
 
users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("Parquet OK:", users_silver.count(), "users,", orders_silver.count(), "orders")
 
# ---------------------------------------------------------------------------
section("Write the silver layer as Iceberg tables")
 
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
 
spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
""").show(truncate=False)
 
spark.sql("""
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lab.silver.orders.files
""").show(truncate=False)
 
users_iceberg = spark.table("lab.silver.users")
orders_iceberg = spark.table("lab.silver.orders")
assert users_iceberg.count() == users_bronze.count()
assert orders_iceberg.count() == stg_orders.count()
print("Iceberg OK:", users_iceberg.count(), "users,", orders_iceberg.count(), "orders")
 
# ---------------------------------------------------------------------------
section("Exercise 1: birthdate_is_valid")
 
stg_orders.createOrReplaceTempView("stg_orders")
 
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))
users_ex1 = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn(
        "birthdate_is_valid",
        # Users without any order are considered valid
        F.coalesce(F.col("birthdate") <= F.to_date("first_order_at"), F.lit(True)),
    )
    .drop("first_order_at")
)
 
users_ex1_sql = spark.sql("""
    SELECT u.*,
           coalesce(u.birthdate <= to_date(f.first_order_at), true) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN (
        SELECT user_id, min(ordered_at) AS first_order_at
        FROM stg_orders
        GROUP BY user_id
    ) f ON u.user_id = f.user_id
""")
 
users_ex1.groupBy("birthdate_is_valid").count().show()
users_ex1.filter(~F.col("birthdate_is_valid")) \
    .select("user_id", "username", "birthdate").show(truncate=False)
 
a = users_ex1.select("user_id", "birthdate_is_valid")
b = users_ex1_sql.select("user_id", "birthdate_is_valid")
print("DataFrame vs SQL differences:", a.exceptAll(b).count() + b.exceptAll(a).count())
 
# ---------------------------------------------------------------------------
section("Exercise 2: zip_code with a Python UDF and a pandas_udf")
 
ZIP_RE = re.compile(r"([A-Z]{2}) ([0-9]{5})$")
 
 
def zip_py(address):
    lines = (address or "").split("\n")
    match = ZIP_RE.search(lines[1]) if len(lines) > 1 else None
    return match.group(2) if match else ""
 
 
zip_udf = F.udf(zip_py, "string")
zip_regexp = F.regexp_extract(
    F.element_at(F.split("address", "\n"), 2), "([A-Z]{2}) ([0-9]{5})$", 2
)
 
 
def bench(label, expr, runs=5):
    """Time a full computation of one column. The noop sink forces every row to be computed."""
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        users_bronze.select(expr.alias("zip")).write.format("noop").mode("overwrite").save()
        times.append(time.perf_counter() - start)
    print(f"{label:<15} best {min(times):.3f} s, mean {sum(times) / len(times):.3f} s")
 
 
bench("regexp_extract", zip_regexp)
bench("python udf", zip_udf("address"))
print("udf vs regexp mismatches:", users_bronze.filter(zip_udf("address") != zip_regexp).count())
 
try:
    import pandas as pd
 
    @F.pandas_udf("string")
    def zip_pandas(address: pd.Series) -> pd.Series:
        line2 = address.fillna("").str.split("\n").str[1]
        return line2.str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1].fillna("")
 
    bench("pandas_udf", zip_pandas("address"))
    print("pandas_udf vs regexp mismatches:",
          users_bronze.filter(zip_pandas("address") != zip_regexp).count())
except Exception as exc:  # pandas or pyarrow missing
    print("pandas_udf not available:", type(exc).__name__, exc)
 
# ---------------------------------------------------------------------------
section("Exercise 3: cache the bronze DataFrames")
 
 
def timed_count(df):
    start = time.perf_counter()
    df.count()
    return time.perf_counter() - start
 
 
print(f"without cache: users {timed_count(users_bronze):.3f} s, "
      f"orders {timed_count(orders_bronze):.3f} s")
users_bronze.cache()
orders_bronze.cache()
users_bronze.count()
orders_bronze.count()  # fills the cache
print(f"with cache:    users {timed_count(users_bronze):.3f} s, "
      f"orders {timed_count(orders_bronze):.3f} s")
 
# Storage tab of the Spark UI, read through its REST API
storage_url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/storage/rdd"
for rdd in json.load(urllib.request.urlopen(storage_url)):
    print(f"  cached: {rdd['name'][:60]} "
          f"{rdd['numCachedPartitions']}/{rdd['numPartitions']} partitions, "
          f"{rdd['memoryUsed']} bytes")
 
# ---------------------------------------------------------------------------
section("Exercise 4: append and time travel")
 
snapshot_before = spark.sql("""
    SELECT snapshot_id FROM lab.silver.orders.snapshots
    ORDER BY committed_at DESC LIMIT 1
""").first()[0]
 
new_order = spark.createDataFrame(
    [("ex4-new-order", stg_orders.first()["user_id"], datetime(2026, 10, 28, 12, 0), 1, "bread")],
    schema=stg_orders.schema,
)
new_order.writeTo("lab.silver.orders").append()
 
spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
""").show(truncate=False)
print("Iceberg, current version:", spark.table("lab.silver.orders").count())
print("Iceberg, before append:  ",
      spark.sql(f"SELECT * FROM lab.silver.orders VERSION AS OF {snapshot_before}").count())
 
# Same append on a copy of the Parquet output, so the silver directory used for the
# dbt comparison stays untouched. A Parquet directory keeps no history.
ex4_path = f"s3a://{BUCKET}/silver/spark/orders_ex4.parquet"
stg_orders.write.mode("overwrite").parquet(ex4_path)
print("Parquet before append:", spark.read.parquet(ex4_path).count())
new_order.write.mode("append").parquet(ex4_path)
print("Parquet after append: ", spark.read.parquet(ex4_path).count())
 
spark.stop()
