val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "1.1.0",
  "org.apache.kafka" % "kafka-streams" % "1.1.0",
  "com.lightbend" %% "kafka-streams-scala" % "0.2.0",
  //"com.lightbend" %% "kafka-streams-query" % "0.1.1",
  //"org.json4s" %% "json4s-jackson" % "3.5.3",
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.0.0-M1",
  "com.typesafe.play" %% "play-ws-standalone" % "2.0.0-M1",
  "com.typesafe.play" %% "play-ws-standalone-json" % "2.0.0-M1",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.1"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.1"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.9.1"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.1"

resolvers ++= Seq(
  "google" at "https://maven-central-asia.storage-download.googleapis.com/repos/central/data/",
  "aliyun" at "http://maven.aliyun.com/nexus/content/groups/public/"
)

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
//scalacOptions := Seq("-Xexperimental", "-unchecked", "-deprecation", "-Ywarn-unused-import")