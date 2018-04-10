## Chapter 3: Basic Structured Operations

### Partitions

To allow every executor to perform work in parallel, Spark breaks up the data into chunks called partitions. A partition is a collection of rows that sit on one physical machine in your cluster.

### Transformations

Spark 中的数据集都是不可变的，这意味着你在创建数据集后就不可能再修改它。但是你可以通过转换在此基础上创建新的数据集。

#### narrow transformations

Only one partition contributes to at most one output partition.(like operation `filter`)

![narrow transformations](/images/spark-03.png)

#### wide transformations

Input partitions contributing to many output partitions.(like operation `shuffle`)

![wide transformations](/images/spark-04.png)

`shuffle`:

简单来说，就是将分布在集群中多个节点上的同一个key，拉取到同一个节点上，进行聚合或join等操作。比如reduceByKey、join等算子，都会触发shuffle操作。shuffle过程中，各个节点上的相同key都会先写入本地磁盘文件中，然后其他节点需要通过网络传输拉取各个节点上的磁盘文件中的相同key。而且相同key都拉取到同一个节点进行聚合操作时，还有可能会因为一个节点上处理的key过多，导致内存不够存放，进而溢写到磁盘文件中。因此在shuffle过程中，可能会发生大量的磁盘文件读写的IO操作，以及数据的网络传输操作。磁盘IO和网络数据传输也是shuffle性能较差的主要原因。

### Data Sources

Spark支持的数据源：

* CSV
* JSON
* Parquet
* ORC
* JDBC/ODBC connections
* Plain-text files
* 还有很多社区贡献的数据源

### Read data

读取文件下下的所有 csv 文件到 dataframe 中，然后创建一个临时表“dfTable”:

```scala
val df = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("data/retail-data/by-day/*.csv")
df.createOrReplaceTempView("dfTable")
```

### Aggregations

* count

```scala
import org.apache.spark.sql.functions.count
df.select(count("StockCode")).show()
```

等价于：

```sql
SELECT COUNT(*) FROM dfTable
```

* countDistinct

```scala
import org.apache.spark.sql.functions.countDistinct
df.select(countDistinct("StockCode")).show()
```

等价于:

```sql
SELECT COUNT(DISTINCT *) FROM DFTABLE
```

* approx_count_distinct

在对准确性要求不高但是希望在更短的时间内获得结果的情况下使用

```scala
import org.apache.spark.sql.functions.approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show()
```

等价于:

```sql
SELECT approx_count_distinct(StockCode, 0.1) FROM DFTABLE
```

* min and max

```scala
import org.apache.spark.sql.functions.{min, max}
df.select(min("Quantity"), max("Quantity")).show()
```

等价于:

```sql
SELECT min(Quantity), max(Quantity) FROM dfTable
```

* sum

```scala
import org.apache.spark.sql.functions.sum
df.select(sum("Quantity")).show()
```

等价于:

```sql
SELECT sum(Quantity) FROM dfTable
```

* sumDistinct

```scala
import org.apache.spark.sql.functions.sumDistinct
df.select(sumDistinct("Quantity")).show()
```

等价于:

```sql
SELECT sumDistinct(Quantity) FROM dfTable
```

* avg

```scala
import org.apache.spark.sql.functions.{sum, count, avg, expr}

df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))
  .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()
```

* Variance and Standard Deviation

```scala
import org.apache.spark.sql.functions.{var_pop, stddev_pop}
import org.apache.spark.sql.functions.{var_samp, stddev_samp}
df.select(var_pop("Quantity"), var_samp("Quantity"),
  stddev_pop("Quantity"), stddev_samp("Quantity")).show()
```

等价于:

```sql
SELECT var_pop(Quantity), var_samp(Quantity),
  stddev_pop(Quantity), stddev_samp(Quantity)
FROM dfTable
```

* Aggregating to Complex Types

```scala
import org.apache.spark.sql.functions.{collect_set, collect_list}
df.agg(collect_set("Country"), collect_list("Country")).show()
```

等价于:

```sql
SELECT collect_set(Country), collect_set(Country) FROM dfTable
```

* groupby

```scala
df.groupBy("InvoiceNo", "CustomerId").count().show()
```

等价于:

```sql
SELECT count(*) FROM dfTable GROUP BY InvoiceNo, CustomerId
```