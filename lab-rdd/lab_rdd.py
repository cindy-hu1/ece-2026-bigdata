import re
from operator import add
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("partitions :", users_lines.getNumPartitions(), orders_lines.getNumPartitions())

header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print("orders (sans en-tête) :", orders_data.count())

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("users lignes brutes :", users_lines.count(), "| users réels :", users_records.count())

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

user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print("reduceByKey :", user_quantity.take(5))

user_quantity_grouped = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .groupByKey()
    .mapValues(sum)
)
print("groupByKey :", user_quantity_grouped.take(5))