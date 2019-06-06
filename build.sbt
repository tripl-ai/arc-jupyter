import Dependencies._

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    name := "arc-jupyter",
    organization := "ai.tripl",
    organizationHomepage := Some(url("https://arc.tripl.ai")),
    scalaVersion := "2.11.12",
    licenses := List("MIT" -> new URL("https://opensource.org/licenses/MIT")),
    libraryDependencies ++= etlDeps,
    parallelExecution in Test := false,
    buildInfoKeys := Seq[BuildInfoKey](version, scalaVersion),
    buildInfoPackage := "ai.tripl.arc.jupyter",
    publishTo := sonatypePublishTo.value,
    pgpPassphrase := Some(sys.env.get("PGP_PASSPHRASE").getOrElse("").toCharArray),
    pgpSecretRing := file("/pgp/secring.asc"),
    pgpPublicRing := file("/pgp/pubring.asc"),
    updateOptions := updateOptions.value.withGigahorse(false)
  )

scalacOptions := Seq("-target:jvm-1.8", "-unchecked", "-deprecation")