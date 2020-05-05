import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "3.0.0"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  val sparkMl = "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"

  val kernel = "sh.almond" %% "kernel" % "0.9.1"
  val caseApp = "com.github.alexarchambault" %% "case-app" % "2.0.0-M16"

  val arc = "ai.tripl" %% "arc" % "3.0.0" % "provided"

  val graph = "ai.tripl" %% "arc-graph-pipeline-plugin" % "1.2.0" % "provided"
  
  val etlDeps = Seq(
    sparkCore,
    sparkSql,
    sparkHive,
    sparkMl,
    kernel,
    caseApp,
    arc,
    graph
  )
}