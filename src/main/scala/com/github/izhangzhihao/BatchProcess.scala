package com.github.izhangzhihao

import org.apache.spark.sql._

/**
  * Data taken from <a href="http://archive.ics.uci.edu/ml/datasets/Online+Retail"/>
  * This is a transnational data set which contains all the transactions occurring
  * between 01/12/2010 and 09/12/2011 for a UK-based and registered non-store online retail.
  * The company mainly sells unique all-occasion gifts. Many customers of the company are wholesalers.
  */
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
    .repartition(8)

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
    .filter('Country === "United Kingdom") //restrict the data to only United Kingdom customers, which contains most of our customers historical data.
    .filter($"Quantity" > 0) //remove canceled orders
    .filter('InvoiceDate gt lit("2010-12-09")) // restrict the data to one full year because it's better to use a metric per Months or Years
    .where("CustomerID is not null") //remove rows where customerID are NA


  retailData.printSchema()

  retailData.show()

  retailData.createOrReplaceTempView("retail")

  spark.sql(
    """
                   SELECT COUNT(DISTINCT InvoiceNo) as Number_of_transactions FROM retail
        """).show()

  spark.sql(
    """
                   SELECT COUNT(DISTINCT StockCode) as Number_of_products_bought FROM retail
        """).show()

  spark.sql(
    """
                   SELECT COUNT(DISTINCT CustomerID) as Number_of_customers FROM retail
        """).show()

  retailData.select(countDistinct('CustomerID).as("Number of customers")).show()

  retailData.select(max('InvoiceDate) as "last date").show()

  val retailDataWithDate: DataFrame = retailData.withColumn("date", to_date('InvoiceDate))
  retailDataWithDate.show(5)

  val retailDataWithLasePurchaseDate: Dataset[Row] = retailDataWithDate
    .groupBy('CustomerID)
    .agg(max('date) as "LastPurchaseDate")

  retailDataWithLasePurchaseDate.show()

  //To calculate recency, we need to choose a date point from which we evaluate how many days ago was the customer's last purchase.

  val retailDataWithRecency: DataFrame = retailDataWithLasePurchaseDate
    .withColumn("today", to_date(expr(""""2011-12-09""""))) //Should be simplified
    .withColumn("Recency", datediff('today, 'LastPurchaseDate))
    .drop("today", "LastPurchaseDate")
  retailDataWithRecency.show()

  //Frequency helps us to know how many times a customer purchased from us.
  //To do that we need to check how many invoices are registered by the same customer.

  val retailDataWithFrequency: DataFrame = retailData
    .groupBy('CustomerID)
    .count()
    .withColumnRenamed("count", "Frequency")

  retailDataWithFrequency.show()


  //Monetary attribute answers the question: How much money did the customer spent over time?

  val retailDateWithMonetary: DataFrame = retailData
    .withColumn("cost", expr("Quantity * UnitPrice"))
    .groupBy('CustomerID)
    .sum("cost")
    .withColumnRenamed("sum(cost)", "Monetary")

  retailDateWithMonetary.show()


  //Create RFM Table

  val RFMTable: DataFrame = retailDataWithRecency
    .join(retailDataWithFrequency, "CustomerID")
    .join(retailDateWithMonetary, "CustomerID")

  RFMTable.show()

  retailData.where('CustomerID === "13832").show()

  //Applying 80-20 rule

  RFMTable.createOrReplaceTempView("RFMTable")

  spark.sql(
    """
      SELECT sum(Monetary) * 0.8 as `The 80% of total revenue` FROM RFMTable
    """).show()


  RFMTable
    .orderBy(desc("Monetary"))
    .limit((3863 * 0.2).toInt)
    .select(sum('Monetary) as "The revenue of top 20% of customers")
    .show()

  // 5,646,700.091200001 VS 5,158,179.370999999
  //In our case, the 80% of total revenue is not achieved by the 20% of TOP customers but approximately, it does, because they are less than our 20% TOP customers who achieve it. It would be interesting to study this group of customers because they are those who make our most revenue.

  //Applying RFM score formula

  private def getQuartiles(col: String) = {
    Array(0.25, 0.50, 0.75).zip(RFMTable.stat.approxQuantile(col, Array(0.25, 0.50, 0.75), 0.05)).toMap
  }

  val recencyQuartiles = getQuartiles("Recency")
  val frequencyQuartiles = getQuartiles("Frequency")
  val monetaryQuartiles = getQuartiles("Monetary")

  object UDFUtils extends Serializable {

    //We will create two segmentation classes since, high recency is bad, while high frequency and monetary value is good.

    /**
      * For recency
      *
      * @param value
      * @param dict
      * @return
      */
    def RScore(value: Double, dict: Map[Double, Double]) =
      value match {
        case v if v <= dict(0.25) => 4
        case v if v <= dict(0.50) => 3
        case v if v <= dict(0.75) => 2
        case _ => 1
      }

    /**
      * For frequency and monetary
      *
      * @param value
      * @param dict
      * @return
      */
    def FMScore(value: Double, dict: Map[Double, Double]) =
      value match {
        case v if v <= dict(0.25) => 1
        case v if v <= dict(0.50) => 2
        case v if v <= dict(0.75) => 3
        case _ => 4
      }


    def R_Quartile(value: Double) = {
      RScore(value, recencyQuartiles)
    }

    //DSL : Usage
    val R_QuartileUDF = udf[Int, Double](R_Quartile)

    // Works With Sql Expr and DSL API
    val R_QuartileUDFRegister = spark.udf.register("R_Quartile", R_QuartileUDF)

    def F_Quartile(value: Double) = {
      FMScore(value, frequencyQuartiles)
    }

    val F_QuartileUDF = udf[Int, Double](F_Quartile)
    val F_QuartileUDFRegister = spark.udf.register("F_Quartile", F_QuartileUDF)

    def M_Quartile(value: Double) = {
      FMScore(value, monetaryQuartiles)
    }

    val M_QuartileUDF = udf[Int, Double](M_Quartile)
    val M_QuartileUDFRegister = spark.udf.register("M_Quartile", M_QuartileUDF)
  }

  import UDFUtils._

  RFMTable
    .withColumn("R_Quartile", R_QuartileUDF('Recency))
    .withColumn("F_Quartile", expr("F_Quartile(Frequency)"))
    .withColumn("M_Quartile", callUDF("M_Quartile", 'Monetary))
    .show()


  //retailData.write.json("data/retail-data/all/online-retail-dataset")
}
