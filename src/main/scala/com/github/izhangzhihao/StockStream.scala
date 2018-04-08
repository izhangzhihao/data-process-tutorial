package com.github.izhangzhihao

import java.util.concurrent.Executors

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.storage.StorageLevel

import scala.concurrent.JavaConversions.asExecutionContext
import scala.concurrent.{ExecutionContextExecutorService, Future}

/**
  * https://www.zybuluo.com/yangzhou/note/1052477
  */
object StockStream extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("StockStream")
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
  spark.sql(s"CREATE TABLE IF NOT EXISTS $hiveDb.stock_indexes (key STRING, type STRING, value DOUBLE, time TIMESTAMP) USING HIVE")

  case class Stock(key: String, `type`: String, value: Double, split: String)

  val result = df.selectExpr("CAST(value AS STRING)")
    .as[String]
    .map { content =>
      val key = content.split(",")(0)
      val value = content
      spark.sql(s"INSERT INTO $hiveDb.stock_parameters VALUES ('$key', '$value')")

      val indexs: Seq[Stock] = List(
        Stock(key,
          "AMO",
          AmoStockIndex.getValue(value.split(",")),
          value.split(",")(32)),
        Stock(key,
          "OBV",
          ObvStockIndex.getValue(value.split(",")),
          value.split(",")(32))
      )

      indexs.toDF().createOrReplaceTempView("stock_index_views")

      spark.sql(s"INSERT INTO $hiveDb.stock_indexes SELECT * FROM stock_index_views")
      indexs
    }

  result.printSchema()

  //  root
  //  |-- value: array (nullable = false)
  //  |    |-- element: struct (containsNull = true)
  //  |    |    |-- key: string (nullable = true)
  //  |    |    |-- type: string (nullable = true)
  //  |    |    |-- value: double (nullable = false)
  //  |    |    |-- split: string (nullable = true)


  import org.apache.spark.sql.functions._

  val streamingQuery: StreamingQuery = result
    //.selectExpr($"value".getItem("_1").as('key), "to_json(struct(*)) AS value")
    //.select('value.getItem("element").getItem("_1") as 'key, to_json(struct('value.getItem("element").getItem("*"))) as 'value)
    .select(explode('value))
    .select($"col.key" as 'key, $"col.value" as 'value)
    .as[(String, String)]
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "hdp2:6667,hdp3:6667,hdp4:6667")
    .option("topic", "stock-indexes_zhihao")
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
