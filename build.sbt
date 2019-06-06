import Dependencies._

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    name := "arc-jupyter",
    organization := "ai.tripl",
    scalaVersion := "2.11.12",
    libraryDependencies ++= etlDeps,
    parallelExecution in Test := false,
    buildInfoKeys := Seq[BuildInfoKey](version, scalaVersion),
    buildInfoPackage := "ai.tripl.arc.jupyter",
    publishMavenStyle := true
  )

scalacOptions := Seq("-target:jvm-1.8", "-unchecked", "-deprecation")
