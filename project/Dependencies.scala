import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "3.1.1"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  val sparkMl = "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"

  val kernel = "sh.almond" %% "kernel" % "0.11.0"

  val arc = "ai.tripl" %% "arc" % "3.8.0" % "provided"

  val etlDeps = Seq(
    sparkCore,
    sparkSql,
    sparkHive,
    sparkMl,
    kernel,
    arc
  )
}