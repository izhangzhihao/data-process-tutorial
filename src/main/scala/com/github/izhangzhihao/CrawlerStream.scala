package com.github.izhangzhihao

import java.util.Date
import java.util.concurrent.Executors

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.storage.StorageLevel
import org.json4s.JValue

import scala.concurrent.JavaConversions.asExecutionContext
import scala.concurrent.{ExecutionContextExecutorService, Future}

object CrawlerStream extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("CrawlerStream")
    .enableHiveSupport()
    .getOrCreate()

//  val spark = SparkSession
//    .builder()
//    .appName("CrawlerStream")
//    .master("local[*]")
//    .getOrCreate()

  import spark.implicits._

  val hiveTable = "data_stats_gov_cn"
  spark.sql(s"""CREATE TABLE IF NOT EXISTS default.$hiveTable
       (value DOUBLE, quotaName STRING,
       quotaType array<STRING>, `object` STRING,
       period STRING, date STRING,
       columnName STRING, source STRING
       ) USING hive""")

  case class Quota(value: Double,
                   quotaName: String,
                   quotaType: Array[String],
                   `object`: String,
                   period: String,
                   date: String,
                   columnName: String,
                   source: String)

  def stringToQuotas(value: String, columnName: String): List[Quota] = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val f = org.json4s.DefaultFormats
    val jValue: JValue = parse(value) \ "returndata"
    val wdNodes: List[JValue] = (jValue \ "wdnodes").extract[List[JValue]]
    val codeToZB = extractCodeMap(wdNodes, "zb")
    val codeToSJ = extractCodeMap(wdNodes, "sj")

    val dataNodes: List[JValue] = (jValue \ "datanodes").extract[List[JValue]]

    def processNodes(it: JValue): Quota = {
      val hasData = (it \ "data" \ "hasdata").extract[Boolean]
      if (!hasData) {
        return null
      }
      val data = (it \ "data" \ "data").extract[Double]
      val zb = codeToZB(
        ((it \ "wds")
          .extract[List[JValue]]
          .filter(wd => (wd \ "wdcode").extract[String] == "zb")
          .head \ "valuecode").extract[String]
      )
      if (zb.isEmpty) {
        return null
      }
      val quotaName = (zb.head \ "cname").extract[String]
      if (quotaName == null || quotaName.isEmpty) {
        return null
      }
      val `object` = if (columnName.contains("hg")) "全国" else "TODO"
      val period =
        if (columnName.contains("yd")) "月度"
        else if (columnName.contains("jd")) "季度"
        else "年度"
      val date = (codeToSJ(
        ((it \ "wds")
          .extract[List[JValue]]
          .filter(it => (it \ "wdcode").extract[String] == "sj")
          .head \ "valuecode").extract[String]
      ).head \ "cname").extract[String]
      val source = "data.stats.gov.cn"
      Quota(
        data,
        quotaName,
        Array.empty,
        `object`,
        period,
        date,
        columnName,
        source
      )
    }

    dataNodes.par
      .map(processNodes)
      .toList
      .filter(_ != null)
  }

  private def extractCodeMap(wdNodes: List[JValue],
                             `type`: String): Map[String, List[JValue]] = {
    implicit val f = org.json4s.DefaultFormats
    (wdNodes
      .filter(it => (it \ "wdcode").extract[String] == `type`)
      .head \ "nodes")
      .extract[List[JValue]]
      .groupBy(it => (it \ "code").extract[String])
  }

  def extractColumnName(key: String): String = {
    val strings = key
      .split("\\+")
    strings(1)
      .split("&")
      .map(_.split("=", 2))
      .filter(_(0) == "dbcode")
      .flatten
      .tail
      .head
  }

  spark.read
    .format("kafka")
    .option(
      "kafka.bootstrap.servers",
      "twdp-dn5:9092,twdp-dn4:9092,twdp-dn3:9092"
    )
    .option("subscribe", "crawler")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
    .flatMap { it =>
      val columnName = extractColumnName(it._1)
      stringToQuotas(it._2, columnName)
    }
    .write
    .format("hive")
    .mode(SaveMode.Append)
    .saveAsTable("data_stats_gov_cn")
}
