import ReleaseTransformations._

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.11.7", "2.10.5", "2.12.0-M2")

organization in ThisBuild := "com.trueaccord.lenses"

scalacOptions in ThisBuild ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, v)) if v <= 11 => List("-target:jvm-1.7")
    case _ => Nil
  }
}

releaseCrossBuild := true

releasePublishArtifactsAction := PgpKeys.publishSigned.value

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  ReleaseStep(action = Command.process("publishSigned", _), enableCrossBuild = true),
  setNextVersion,
  commitNextVersion,
  ReleaseStep(action = Command.process("sonatypeReleaseAll", _), enableCrossBuild = true),
  pushChanges
)

lazy val root = project.in(file("."))
  .aggregate(lensesJS, lensesJVM)
  .settings(
    publish := {},
    publishLocal := {},
    publishArtifact := false
  )

lazy val lenses = crossProject.in(file("."))
  .settings(
    name := "lenses"
  )
  .jvmSettings(
    libraryDependencies ++= Seq(
      "org.scalacheck" %%% "scalacheck" % "1.12.5" % "test",
      "org.scalatest" %%% "scalatest" % (if (scalaVersion.value.startsWith("2.12")) "2.2.5-M2" else "2.2.5") % "test"
    )
  )
  .jsSettings(
  )

lazy val lensesJVM = lenses.jvm
lazy val lensesJS = lenses.js

