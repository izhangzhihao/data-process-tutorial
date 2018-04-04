package com.github.izhangzhihao

import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.scalatest._

abstract class SparkSpec extends FreeSpec with BeforeAndAfterEach with Matchers {
  val sparkSession = SparkSession
    .builder()
    .appName("testing")
    .master("local[*]")
    .config("spark.driver.allowMultipleContexts", "false")
    .getOrCreate()

  object testImplicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = sparkSession.sqlContext
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }
}

