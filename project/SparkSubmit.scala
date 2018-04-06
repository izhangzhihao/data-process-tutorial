import sbtsparksubmit.SparkSubmitPlugin.autoImport.SparkSubmitSetting

object SparkSubmit {
  lazy val settings = SparkSubmitSetting(
    SparkSubmitSetting("BatchProcess",
      Seq("--class", "com.github.izhangzhihao.BatchProcess")
    )
  )
}

