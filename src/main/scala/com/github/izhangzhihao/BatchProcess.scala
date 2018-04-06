package com.github.izhangzhihao

import org.apache.spark.sql.SparkSession

object BatchProcess extends App {

  val spark = SparkSession.builder()
    .appName("BatchProcess")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._

  val originData = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("data/retail-data/by-day/*.csv")
    .repartition(2)

  originData.printSchema()

  val retailData = originData
    .select(
      $"InvoiceNo",
      $"StockCode",
      $"Description",
      $"Quantity",
      date_format($"InvoiceDate", "yyyy-MM-dd HH:mm:ss").cast(DataTypes.TimestampType).as("InvoiceDate"),
      $"UnitPrice",
      $"CustomerID".cast("int"),
      $"Country"
    )
    .filter("CustomerID is not null") // where === filter


  retailData.printSchema()

  retailData.show(10)

  retailData.write.json("data/retail-data/all/online-retail-dataset")
}
