package com.github.izhangzhihao

import java.net.InetAddress
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
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val host = InetAddress.getLocalHost.getHostAddress

  val df: DataFrame =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$host:9092")
      .option("subscribe", "stock-mins")
      .option("startingOffsets", "earliest")
      .load()
  df.printSchema()

  //df.persist(StorageLevel.MEMORY_ONLY)

  case class Stock(key: String, `type`: String, value: Double, split: String)

  val result = df.selectExpr("CAST(value AS STRING)")
    .as[String]
    .map { content =>
      val key = content.split(",")(0)
      val value = content

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

  val streamingQuery = result
    .select(explode('value))
    .select($"col.key" as 'key, $"col.value" as 'value)
    .as[(String, Double)]
    .writeStream
    .queryName("kv")
    .format("memory")
    .start()

  implicit val context: ExecutionContextExecutorService = asExecutionContext(Executors.newSingleThreadExecutor())
  val eventual = Future {
    try {
      streamingQuery.awaitTermination()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  streamingQuery.explain()
  println()

  Thread.sleep(10000)

  spark.sql("select * from kv").show()
  streamingQuery.stop()
}
