import sbt._

object Dependencies {
  // Versions
  lazy val sparkVersion = "2.4.3"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion 
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion 
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion
  val sparkMl = "org.apache.spark" %% "spark-mllib" % sparkVersion 

  val kernel = "sh.almond" %% "kernel" % "0.5.0"
  val caseApp = "com.github.alexarchambault" %% "case-app" % "2.0.0-M8"

  val arc = "ai.tripl" %% "arc" % "2.0.0"

  val etlDeps = Seq(
    sparkCore,
    sparkSql,
    sparkHive,
    sparkMl,
    kernel,
    caseApp,
    arc
  )
}