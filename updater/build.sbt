
ThisBuild / scalaVersion     := "2.13.3"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "lv.uniso"
ThisBuild / organizationName := "Uniso"

lazy val deps = {
  Seq(
    "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0",
    "org.postgresql" % "postgresql" % "42.2.16",
  )
}

lazy val root = (project in file("."))
  .settings(
    name := "updater",
    libraryDependencies ++= deps
  )
