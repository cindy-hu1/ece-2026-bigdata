"""Lab: Introduction to Spark's RDD API.

Run from the repository root, once users.csv and orders.csv have been
downloaded from the bronze bucket:

    spark-submit lab-rdd/lab_rdd.py
"""

import json
import re
import time
import urllib.request
from operator import add

from pyspark.sql import SparkSession

USERS_PATH = "users.csv"
ORDERS_PATH = "orders.csv"

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


def section(title):
    print(f"\n===== {title} =====")


def spark_ui_api(endpoint):
    """Query the Spark UI REST API (the web UI is not reachable behind the Onyxia proxy)."""
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{endpoint}"
    return json.load(urllib.request.urlopen(url))


# ---------------------------------------------------------------------------
section("RDD creation and partitions")

users_lines = sc.textFile(USERS_PATH)
orders_lines = sc.textFile(ORDERS_PATH)
print("users_lines partitions :", users_lines.getNumPartitions())
print("orders_lines partitions:", orders_lines.getNumPartitions())

orders_repartitioned = orders_lines.repartition(8)
print("after repartition(8)   :", orders_repartitioned.getNumPartitions())

# ---------------------------------------------------------------------------
section("Lazy evaluation and lineage")

header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
print("orders_data.count() :", orders_data.count())
print("second count        :", orders_data.count())

# ---------------------------------------------------------------------------
section("The cost of no schema: the multiline address")

for line in users_lines.take(6):
    print(line)
print("users_lines.count()   :", users_lines.count())

UUID_AT_START = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("users_records.count() :", users_records.count())

# ---------------------------------------------------------------------------
section("Manual parsing of orders")


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


orders_rdd = orders_data.map(parse_order).cache()
print(orders_rdd.take(3))

# ---------------------------------------------------------------------------
section("Key-value RDDs: total quantity per user")

user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print("reduceByKey :", user_quantity.take(5))

user_quantity_grouped = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .groupByKey()
    .mapValues(sum)
)
print("groupByKey  :", user_quantity_grouped.take(5))

print("\nShuffle size per stage (bytes):")
for stage in spark_ui_api("stages"):
    print(
        f"  stage {stage['stageId']:>2}  "
        f"read={stage['shuffleReadBytes']:>6}  write={stage['shuffleWriteBytes']:>6}"
    )

# ---------------------------------------------------------------------------
section("Manual deduplication")


def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


orders_dedup = (
    orders_rdd
    .map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)
print("before / after dedup :", orders_rdd.count(), orders_dedup.count())

# ---------------------------------------------------------------------------
section("Joining orders with users")


def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username


users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))

enriched = orders_kv.join(users_kv)
print("joined orders :", enriched.count())
for uid, (order, username) in enriched.take(3):
    print(" ", uid, order["product"], username)

# ---------------------------------------------------------------------------
section("Exercise 1: aggregateByKey")

stats = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .aggregateByKey(
        (0, 0),                                       # (order_count, total_quantity)
        lambda acc, q: (acc[0] + 1, acc[1] + q),      # within a partition
        lambda a, b: (a[0] + b[0], a[1] + b[1]),      # across partitions
    )
)
avg_qty = stats.mapValues(lambda s: s[1] / s[0])
print("average quantity per user :", avg_qty.take(5))

# ---------------------------------------------------------------------------
section("Exercise 2: orders with no matching user")

fake_order = {
    "order_id": "X1",
    "user_id": "fake-uuid",
    "date": "2026-01-01",
    "quantity": 1,
    "product": "test",
}
orders_kv_with_fake = orders_kv.union(sc.parallelize([("fake-uuid", fake_order)]))
left = orders_kv_with_fake.leftOuterJoin(users_kv)
orphans = left.filter(lambda kv: kv[1][1] is None)
print("orphan orders :", orphans.count())

# ---------------------------------------------------------------------------
section("Exercise 3: DataFrame API")

# The CSV header names the user column `user_uuid`, not `user_id`.
df = spark.read.csv(ORDERS_PATH, header=True, inferSchema=True)
df_sum = df.groupBy("user_uuid").sum("quantity")
df_sum.show(5)
df_sum.explain()
print(user_quantity.toDebugString().decode())

# ---------------------------------------------------------------------------
section("Exercise 4: cache and timing")


def timed_count(rdd):
    start = time.perf_counter()
    rdd.count()
    return time.perf_counter() - start


def show_storage(label):
    rdds = spark_ui_api("storage/rdd")
    print(f"Storage ({label}) : {len(rdds)} cached RDD(s)")
    for r in rdds:
        print(
            f"  {r['name']}: {r['numCachedPartitions']}/{r['numPartitions']} partitions, "
            f"{r['memoryUsed']} bytes in memory"
        )


orders_rdd.count()  # make sure the cache is filled
print(f"with cache    orders_rdd    : {timed_count(orders_rdd):.3f} s")
print(f"with cache    user_quantity : {timed_count(user_quantity):.3f} s")
show_storage("with cache")

orders_rdd.unpersist()  # same effect as removing .cache() from the parsing step
print(f"without cache orders_rdd    : {timed_count(orders_rdd):.3f} s")
print(f"without cache user_quantity : {timed_count(user_quantity):.3f} s")
show_storage("without cache")

spark.stop()