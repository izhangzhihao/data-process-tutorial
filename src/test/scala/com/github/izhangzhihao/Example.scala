package com.github.izhangzhihao

import org.apache.spark.sql.DataFrame

class Example extends SparkSpec {
  var flightData2015: DataFrame = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    flightData2015 = sparkSession
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/flight-data/csv/2015-summary.csv")
  }

  "Spark" - {
    "should be able to work" in {
      val myRange = sparkSession.range(1000).toDF("number")
      val divideBy = myRange.where("number % 2 = 0")
      divideBy.count() should be(500)
    }

    "should be able to load csv file" - {

      "should be able to explain physical plan" in {

        println(flightData2015.take(3))
        println(flightData2015.sort("count").explain())
        //          """
        //            |== Physical Plan ==
        //            |*(2) Sort [count#12 ASC NULLS FIRST], true, 0
        //            |+- Exchange rangepartitioning(count#12 ASC NULLS FIRST, 200)
        //            |   +- *(1) FileScan csv [DEST_COUNTRY_NAME#10,ORIGIN_COUNTRY_NAME#11,count#12] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/Users/zhazhang/Downloads/Spark-The-Definitive-Guide/data/flight-data/csv/..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string,ORIGIN_COUNTRY_NAME:string,count:int>
        //            |()
        //          """.stripMargin
      }

      "should be able to set partitions as config" in {
        sparkSession.conf.set("spark.sql.shuffle.partitions", "5")

        flightData2015.sort("count").take(2)
      }

      "spark sql & DataFrame API should be the same" in {
        flightData2015.createOrReplaceTempView("flight_data_2015")

        println(sparkSession.sql(
          """
         SELECT DEST_COUNTRY_NAME,count(1)
         FROM flight_data_2015
         GROUP BY DEST_COUNTRY_NAME
      """).explain())

        println(
          flightData2015.groupBy("DEST_COUNTRY_NAME")
            .count()
            .explain()
        )

        println(sparkSession.sql("SELECT max(count) from flight_data_2015").take(1).mkString)

        sparkSession.sql(
          """
        SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
        FROM flight_data_2015
        GROUP BY DEST_COUNTRY_NAME
        ORDER BY sum(count) DESC
        LIMIT 5
      """)
          .show()

        import org.apache.spark.sql.functions.desc

        flightData2015
          .groupBy("DEST_COUNTRY_NAME")
          .sum("count")
          .withColumnRenamed("sum(count)", "destination_total")
          .sort(desc("destination_total"))
          .limit(5)
          .show()
      }
    }
  }
}

