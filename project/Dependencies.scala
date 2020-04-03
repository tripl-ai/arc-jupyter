import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "2.4.5"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  val sparkMl = "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"

  val kernel = "sh.almond" %% "kernel" % "0.6.0"
  val caseApp = "com.github.alexarchambault" %% "case-app" % "2.0.0-M9"

  val arc = "ai.tripl" %% "arc" % "2.8.1" % "provided"

  val graph = "ai.tripl" %% "arc-graph-pipeline-plugin" % "1.1.0" % "provided"

  val etlDeps211 = Seq(
    sparkCore,
    sparkSql,
    sparkHive,
    sparkMl,
    kernel,
    caseApp,
    arc
  )
  
  val etlDeps212 = Seq(
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