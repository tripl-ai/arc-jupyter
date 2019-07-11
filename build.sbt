import Dependencies._

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.8"
lazy val supportedScalaVersions = List(scala211, scala212)

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  configs(IntegrationTest).
  settings(
    name := "arc-jupyter",
    organization := "ai.tripl",
    organizationHomepage := Some(url("https://arc.tripl.ai")),
    crossScalaVersions := supportedScalaVersions,
    licenses := List("MIT" -> new URL("https://opensource.org/licenses/MIT")),
    libraryDependencies ++= etlDeps,
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    buildInfoKeys := Seq[BuildInfoKey](version, scalaVersion),
    buildInfoPackage := "ai.tripl.arc.jupyter",
    Defaults.itSettings,
    publishTo := sonatypePublishTo.value,
    pgpPassphrase := Some(sys.env.get("PGP_PASSPHRASE").getOrElse("").toCharArray),
    pgpSecretRing := file("/pgp/secring.asc"),
    pgpPublicRing := file("/pgp/pubring.asc"),
    updateOptions := updateOptions.value.withGigahorse(false)
  )

fork in run := true  

scalacOptions := Seq("-target:jvm-1.8", "-unchecked", "-deprecation")