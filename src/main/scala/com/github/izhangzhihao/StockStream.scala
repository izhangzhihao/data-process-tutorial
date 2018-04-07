package com.github.izhangzhihao

import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

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

  spark.sql("CREATE DATABASE IF NOT EXISTS db_stock_zhihao")
  spark.sql("CREATE TABLE IF NOT EXISTS db_stock_zhihao.stock_parameters (key STRING, value STRING) USING HIVE")
  spark.sql("CREATE TABLE IF NOT EXISTS db_stock_zhihao.stock_indexes (key STRING, type STRING, value DOUBLE, time TIMESTAMP) USING HIVE")

  //implicit val tupleEncoder: Encoder[(String, String)] = org.apache.spark.sql.Encoders.tuple(Encoders.STRING, Encoders.STRING)
  //implicit val tupleEncoder: Encoder[DataFrame] = ???

  case class Stock(key: String, value: String)

  val result = df.selectExpr("CAST(value AS STRING)")
    .as[String]
    .map { content =>
      val key = content.split(",")(0)
      val value = content
      spark.sql(s"INSERT INTO db_stock_zhihao.stock_parameters VALUES ('$key', '$value')")

      val indexs: Seq[(String, String, Double, String)] = List(
        (key,
          "AMO",
          AmoStockIndex.getValue(value.split(",")),
          value.split(",")(32)),
        (key,
          "OBV",
          ObvStockIndex.getValue(value.split(",")),
          value.split(",")(32))
      )

      indexs.toDF().createOrReplaceTempView("stock_index_views")

      spark.sql("INSERT INTO db_stock_zhihao.stock_indexes SELECT * FROM stock_index_views")
      indexs
    }

  result.printSchema()

  result
    .selectExpr("CAST(key AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "hdp2:6667,hdp3:6667,hdp4:6667")
    .option("topic", "stock-indexes_zhihao")
    .option("checkpointLocation", "hdfs:/user/zhihao/kafkaCheckPoint")
    .start()
}
