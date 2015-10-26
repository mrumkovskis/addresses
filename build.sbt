import sbt._
import Keys._

lazy val commonSettings = Seq(
  organization := "lv.addresses",
  scalaVersion := "2.11.6",
  scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8")
)

lazy val serviceDependencies = {
  val akkaV = "2.4.0"
  Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0",
  "com.typesafe.akka" %% "akka-http-core-experimental" % "1.0",
  "com.typesafe.akka" %% "akka-http-experimental" % "1.0",
  "io.spray"            %%  "spray-json"    % "1.3.2",
  "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
  "com.typesafe.akka"   %%  "akka-testkit"  % akkaV  % "test",
  "commons-net" % "commons-net" % "3.3")
}

lazy val indexer = project
  .in(file("indexer"))
  .settings(commonSettings: _*)

lazy val service = project
  .in(file("service"))
  .dependsOn(indexer)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= serviceDependencies,
    resolvers ++= Seq("spray repo" at "http://repo.spray.io/"),
    mainClass in Compile := Some("lv.addresses.service.Boot")
  )
  .settings(Revolver.settings: _*)
  .settings(javaOptions in Revolver.reStart += "-Xmx4G")

lazy val addresses = project
  .in(file("."))
  .aggregate(indexer, service)
  .dependsOn(service)
  .settings(name := "addresses")
  .settings(commonSettings: _*)
  .settings(initialCommands in console := "import lv.addresses.service._")
  .settings(
    aggregate in assembly := false,
    mainClass in assembly := Some("lv.addresses.service.Boot"),
    assemblyMergeStrategy in assembly := {
      case "application.conf" => MergeStrategy.concat
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
  .settings(
    publishTo <<= version { v: String =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { x => false },
    pomExtra := (
      <url>https://github.com/mrumkovskis/addresses</url>
      <licenses>
        <license>
          <name>MIT</name>
          <url>http://www.opensource.org/licenses/MIT</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:mrumkovskis/addresses.git</url>
        <connection>scm:git:git@github.com:mrumkovskis/addresses.git</connection>
      </scm>
      <developers>
        <developer>
          <id>mrumkovskis</id>
          <name>Martins Rumkovskis</name>
          <url>https://github.com/mrumkovskis/</url>
        </developer>
      </developers>
    )
  )
