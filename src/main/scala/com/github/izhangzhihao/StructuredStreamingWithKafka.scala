package com.github.izhangzhihao

import java.net.InetAddress
import java.util.concurrent.Executors

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.concurrent.JavaConversions.asExecutionContext
import scala.concurrent.{ExecutionContextExecutorService, Future}

object StructuredStreamingWithKafka extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("StructuredStreamingWithKafka")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val host = InetAddress.getLocalHost.getHostAddress

  val df: DataFrame =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$host:9092")
      .option("subscribe", "topic1")
      .option("startingOffsets", "earliest")
      .load()
  df.printSchema()

  val value = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  val kvStream: StreamingQuery =
    value
      .writeStream
      //.option("checkpointLocation", "/tmp/checkpoint")
      .queryName("kv")
      .format("memory")
      .outputMode("append")
      .start()
  implicit val context: ExecutionContextExecutorService = asExecutionContext(Executors.newSingleThreadExecutor())
  Future(kvStream.awaitTermination())

  Thread.sleep(5000)

  spark.sql("select * from kv").show()

  System.exit(0)
}
