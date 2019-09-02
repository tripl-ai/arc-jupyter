import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "2.4.4"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion 
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion 
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion
  val sparkMl = "org.apache.spark" %% "spark-mllib" % sparkVersion 

  val kernel = "sh.almond" %% "kernel" % "0.6.0"
  val caseApp = "com.github.alexarchambault" %% "case-app" % "2.0.0-M9"

  val arc = "ai.tripl" %% "arc" % "2.0.1"

  val graph = "ai.tripl" %% "arc-graph-pipeline-plugin" % "1.0.0"

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