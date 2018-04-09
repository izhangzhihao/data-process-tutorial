package com.github.izhangzhihao

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.concurrent.Executors

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.storage.StorageLevel

import scala.concurrent.{ExecutionContextExecutorService, Future}
import scala.concurrent.JavaConversions.asExecutionContext

class StockMACDStream {
  val spark: SparkSession = SparkSession.builder()
    .appName("StockMACDStream")
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setCheckpointDir("hdfs:/user/zhihao/checkpoint")

  import spark.implicits._

  val df: DataFrame =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hdp2:6667,hdp3:6667,hdp4:6667")
      .option("subscribe", "stock-mins")
      .option("startingOffsets", "earliest")
      .load()
  df.printSchema()

  df.persist(StorageLevel.MEMORY_AND_DISK)

  val hiveDb = "db_stock_zhihao"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $hiveDb")
  spark.sql(s"CREATE TABLE IF NOT EXISTS $hiveDb.stock_parameters (key STRING, value STRING) USING HIVE")

  case class Stock(key: String, `type`: String, value: Double, split: String)

  import java.time.format.DateTimeFormatter
  import org.apache.spark.sql.functions._

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss") //2018-04-09 15:00:00

  def MACD(key: String, contents: Array[String], evenTime: String): Stock = {
    Stock(
      key,
      "MACD",
      MACDStockIndex.getValue(contents take 12, contents),
      contents(0).split(",")(32)
    )
  }

  val MACDUDF = udf[Stock, String, Array[String], String](MACD)

  val reduceSumArray = udf[Array[String], Array[String], Array[String]](_ ++ _)

  val result = df.selectExpr("CAST(value AS STRING)")
    .as[String]
    .map { content =>
      val contents = content.split(",")
      val key = contents(0)
      val value = content
      val evenTime = Timestamp.valueOf(LocalDateTime.parse(s"${content(31)} ${content(32)}", formatter)).toString
      spark.sql(s"INSERT INTO $hiveDb.stock_parameters VALUES ('$key', '$value')")

      StockItem(key, Array(value), evenTime)
    }
    //.groupByKey(_._1)
    //.reduceGroups((left, right) => (left._1, left._2 ++ right._2))
    .groupBy(window('eventTime, "26 minutes"), 'key)
    .sum(reduceSumArray('value))
    .agg(MACDUDF('key, 'value, 'evenTime))


  result.printSchema()

  val streamingQuery: StreamingQuery = result
    .select(explode('value))
    .select($"col.key" as 'key, struct($"col.*") as 'value)
    .as[(String, Stock)]
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "hdp2:6667,hdp3:6667,hdp4:6667")
    .option("topic", "stock-index-macd_zhihao")
    .option("checkpointLocation", "hdfs:/user/zhihao/kafkaCheckPoint")
    .start()

  streamingQuery.explain()

  implicit val context: ExecutionContextExecutorService = asExecutionContext(Executors.newSingleThreadExecutor())
  val eventual = Future {
    try {
      streamingQuery.awaitTermination()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}

case class StockItem(key: String, contents: Array[String], evenTime: String)
