Lab: Introduction to Spark's RDD API

ECE 2026, Big Data course. Spark 4.1 in local[*] mode, on the vscode-pyspark Onyxia service.

The lab works on the bronze users.csv and orders.csv datasets with the low-level RDD API only: partitioning, lazy evaluation, key-value aggregations, manual deduplication and joins. The last exercise compares the same aggregation with the DataFrame API.

Content
File	Description
lab_rdd.py	The whole lab and the four exercises in one script
README.md	Results and answers to the lab questions
How to run

From the repository root:

bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" users.csv
aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/orders.csv" orders.csv

spark-submit lab-rdd/lab_rdd.py

rm -f users.csv orders.csv

The CSV files are not committed to the repository.

Results
Step	Result
Partitions of users_lines / orders_lines	2 / 2
Partitions after repartition(8)	8
Orders (header removed)	2829
users_lines.count() / users_records.count()	101 / 50
Orders before / after deduplication	2829 / 2829
Orders with a matching user (join)	2829
Orphan orders (exercise 2)	1
Shuffle size, reduceByKey vs groupByKey

Both versions return the same totals (for instance 12, 40, 253, 57 and 175 for the first five users).

Operation	Stages	Shuffle write	Shuffle read
reduceByKey	5, 6	2 513 B	1 065 B
groupByKey	7, 8	6 230 B	2 643 B

groupByKey writes about 2.5 times more data to the shuffle. The read side is smaller than the write side in both cases because take(5) only reads the partitions it needs, while the map side processes the whole file.

Join sample
f143262f-dc5c-4eed-8da0-365bf89897b9 bread kayla51
f143262f-dc5c-4eed-8da0-365bf89897b9 bread kayla51
f143262f-dc5c-4eed-8da0-365bf89897b9 croissant kayla51
Exercise 1: average quantity per user
('f143262f-dc5c-4eed-8da0-365bf89897b9', 3.0)
('969b6662-0562-4059-968c-c69b1064005c', 3.3333333333333335)
('ce9e1a11-fcbb-4e59-bbdd-cf7c9c96e9ec', 2.976470588235294)
('5563f616-00e8-4ece-8b49-452d46d483f3', 2.7142857142857144)
('f94d6204-6808-493f-9fed-2c43e256a6dc', 3.5714285714285716)
Exercise 3: .explain() vs .toDebugString()

DataFrame physical plan:

AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[user_uuid#18], functions=[sum(quantity#20)])
   +- Exchange hashpartitioning(user_uuid#18, 200), ENSURE_REQUIREMENTS, [plan_id=76]
      +- HashAggregate(keys=[user_uuid#18], functions=[partial_sum(quantity#20)])
         +- FileScan csv [user_uuid#18,quantity#20] ... ReadSchema: struct<user_uuid:string,quantity:int>

RDD lineage:

(2) PythonRDD[69] at RDD at PythonRDD.scala:58 []
 |  MapPartitionsRDD[13] at mapPartitions at PythonRDD.scala:170 []
 |  ShuffledRDD[12] at partitionBy at NativeMethodAccessorImpl.java:0 []
 +-(2) PairwiseRDD[11] at reduceByKey at <string>:34 []
    |  PythonRDD[10] at reduceByKey at <string>:34 []
    |  PythonRDD[8] at RDD at PythonRDD.scala:58 []
    |      CachedPartitions: 2; MemorySize: 168.0 KiB; DiskSize: 0.0 B
    |  orders.csv MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0 []
    |  orders.csv HadoopRDD[2] at textFile at NativeMethodAccessorImpl.java:0 []
Exercise 4: cache and timing
	orders_rdd.count()	user_quantity.count()	Storage tab
With cache	0.191 s	0.194 s	1 RDD, 2/2 partitions, about 168 KiB
Without cache	0.283 s	0.130 s	empty
Answers to the questions
RDD creation and partitions

What decides the number of partitions textFile() picks by default? The number of input splits of the file, with a lower bound given by minPartitions, which defaults to min(defaultParallelism, 2). Both files are much smaller than one block, so Spark falls back to that minimum and creates 2 partitions.

Which of repartition() and coalesce() shuffles data? repartition() always shuffles: it redistributes every row to get balanced partitions, and it can increase or decrease their number. coalesce() does not shuffle by default: it merges neighbouring partitions in place, so it can only reduce their number.

Lazy evaluation and lineage

Does a second orders_data.count() show a new stage? Why? Yes. Every action starts a new job and computes the whole lineage again. Since orders_data is not cached, Spark reads the file and applies the filter a second time.

At which point would .cache() change that answer? Calling orders_data.cache() before the first count(). The first action then fills the cache, and the second one reads the partitions from memory instead of reading the file again.

The cost of no schema: the multiline address

Why does spark.read.csv(multiLine=True) not have this problem? It is a real CSV parser: it understands quotes, so a line break inside a quoted field stays part of that field. textFile() splits on every \n without looking at the content, which turns 50 users into 101 lines.

What has to be true of a file format for textFile() to be safe? Every record must fit on exactly one line, with no line break inside a field (JSON Lines, or CSV without quoted newlines). This also lets the file be split between any two lines to build partitions.

Manual parsing of orders

What happens if one line has an extra comma inside product? line.split(",") returns 6 values instead of 5 and the unpacking raises a ValueError. Because of lazy evaluation the error does not happen at read time but at the first action that reaches that line, and it fails the whole job.

Where does the DataFrame API perform the equivalent check, and when? The CSV reader handles commas inside quoted fields. Column names and types are checked when the plan is analysed, before any action runs: using user_id instead of user_uuid in exercise 3 failed immediately at groupBy. Malformed rows are handled at read time according to the mode option (PERMISSIVE, DROPMALFORMED or FAILFAST).

Key-value RDDs

What does groupByKey shuffle instead, and why is it more expensive? It shuffles every individual (user, quantity) pair and only sums them afterwards, whereas reduceByKey sends one partial sum per user and per partition. More bytes go through disk and network (6 230 B against 2 513 B here), and all the values of one key must fit in memory on a single executor.

For which aggregations is groupByKey unavoidable? When the result needs all the values of a key at once and cannot be built from partial results: a median or percentile, sorting a user's orders, or getting the full list of products per user.

Manual deduplication

What plays the role of partitionBy() here? The shuffle of reduceByKey. It hash-partitions the pairs on order_id, so every order with the same order_id ends up in the same partition before being combined.

Which version states the intent more directly? The window function. It reads as "for each order_id, sort by date descending and keep the first row". The reduceByKey version needs a hand-written combiner, which must be associative and commutative to give a correct result. On this dataset both give 2829 rows: there is no duplicated order_id.

Joining orders with users

What would need to be true for Spark to skip the shuffle? Both RDDs would need to be partitioned with the same partitioner and the same number of partitions (for example partitionBy(n) on the user id, then cached). Rows with the same key are then already in the same partition and the join becomes a narrow dependency.

What changes with leftOuterJoin()? Orders whose user_id has no match are kept instead of dropped, with None as the username: (user_id, (order, None)). Exercise 2 shows it with one injected order.

What is the risk of calling .collect() on enriched? collect() brings every row back to the driver. On a large dataset it runs out of memory, all the more since a join can multiply the number of rows. Filtering or aggregating first, or using take(), avoids it.

Exercises

1. aggregateByKey. A single pass builds (order_count, total_quantity) per user, then mapValues computes the average. Like reduceByKey, aggregateByKey combines values inside each partition before the shuffle.

2. Orphan orders. One fake order with an unknown user_id was added to a copy of orders_kv. After leftOuterJoin(), filtering on a None username returns exactly 1 order.

3. DataFrame API. The RDD version needs the parse_order function, the header filter and the map plus reduceByKey, around fifteen lines. The DataFrame version takes two lines: read and groupBy. The physical plan shows a HashAggregate with partial_sum before the Exchange, then a final HashAggregate: Spark applies on its own the per-partition pre-aggregation that reduceByKey does by hand. It also reads only the two needed columns (ReadSchema). The RDD lineage shows the ShuffledRDD created by reduceByKey and the 2 cached partitions of orders_rdd. Note that the column is named user_uuid in the CSV header, not user_id as in the lab statement.

4. Cache. orders_rdd.count() drops from 0.283 s to 0.191 s with the cache, and the Storage tab lists the cached RDD (2 partitions, about 168 KiB). For user_quantity.count() the cache makes no difference: the shuffle files of the previous job are reused, so Spark skips the stage that reads orders_rdd. On such a small file these gaps stay close to measurement noise.

Notes
The Spark web UI was not reachable through the Onyxia VS Code proxy (/proxy/4040 redirects to a path that does not exist). Shuffle sizes and the Storage tab were read from the Spark UI REST API (/api/v1/applications/<app-id>/stages and /storage/rdd), which returns the same data.
In exercise 4, orders_rdd.unpersist() is used instead of editing the parsing step and rerunning it. The effect on the cache is the same.
