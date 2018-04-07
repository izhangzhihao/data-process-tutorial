package com.github.izhangzhihao

/**
  * https://www.zybuluo.com/yangzhou/note/1052477
  */
object ObvStockIndex {
  def getValue(paras: Array[String]): Double = paras(11).toDouble
}

object AmoStockIndex {
  def getValue(paras: Array[String]): Double = paras(12).toDouble
}

object EMAStockIndex {
  def getValue(paras: Array[String]): Double = {
    val EMA1 = paras(0)(11).toDouble
    var EMAs: Array[Double] = Array(EMA1)
    val k = 2.0 / (paras.length + 1)
    for (i <- paras.indices) {
      val emai = paras(i)(11).toDouble * k + EMAs.last * (1 - k)
      EMAs :+ emai
    }

    EMAs.last
  }
}

object MACDStockIndex {
  def getValue(paras12: Array[String], paras26: Array[String]): Double =
    EMAStockIndex.getValue(paras12) - EMAStockIndex.getValue(paras26)
}
