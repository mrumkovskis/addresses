import sbt._
import Keys._

lazy val commonSettings = Seq(
  organization := "lv.addresses",
  scalaVersion := "2.13.3",
  scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8")
)

val akkaHttpV = "10.2.0"
val akkaV = "2.6.8"

lazy val serviceDependencies = {
  Seq(
    "com.typesafe.akka" %% "akka-http"                         % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-spray-json"              % akkaHttpV,
    "com.lightbend.akka" %% "akka-stream-alpakka-ftp"          % "2.0.2",
    "com.typesafe.akka" %% "akka-slf4j"                        % akkaV,
    "com.typesafe.akka" %% "akka-http-testkit"                 % akkaHttpV  % "test")
}

lazy val indexerDependencies = {
  Seq(
    "com.typesafe.akka" %% "akka-stream"                       % akkaV,
    "ch.qos.logback" % "logback-classic"                       % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging"            % "3.9.2",
    "org.tresql" %% "tresql"                                   % "10.0.0",
    "com.zaxxer" % "HikariCP"                                  % "3.4.2",
  )
}

lazy val updaterDependencies = {
  Seq(
    "com.oracle.ojdbc" % "ojdbc8"  % "19.3.0.0",
    "com.oracle.ojdbc" % "orai18n" % "19.3.0.0",
    "com.h2database"   % "h2"      % "1.4.200",
    "com.typesafe.akka" %% "akka-slf4j" % akkaV,
  )
}

lazy val updater = project
  .in(file("updater"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= updaterDependencies
  )

lazy val indexer = project
  .in(file("indexer"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= indexerDependencies,
    javaOptions += "-Xmx4G", // h2 string_agg query takes a lot
  )

lazy val service = project
  .in(file("service"))
  .dependsOn(indexer)
  .dependsOn(updater)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= serviceDependencies,
    mainClass in Compile := Some("lv.addresses.service.Boot")
  )
  .settings(Revolver.settings: _*)
  .settings(javaOptions in reStart += "-Xmx4G")
  .settings(initialCommands in console := s"""
    |import lv.addresses.service.AddressFinder._
    """.stripMargin)

lazy val addresses = project
  .in(file("."))
  .aggregate(indexer, service)
  .dependsOn(service)
  .settings(name := "addresses")
  .settings(commonSettings: _*)
  .settings(initialCommands in console := s"""
    |import lv.addresses.service._
    |//import akka.actor._
    |//import akka.stream._
    |//import scaladsl._
    |//implicit val system = ActorSystem("test-system")
    |//implicit val materializer = ActorMaterializer()""".stripMargin)
  .settings(
    aggregate in assembly := false,
    // mainClass in assembly := Some("lv.addresses.service.Boot"),
    Compile / run / mainClass := Some("lv.addresses.BootDispatcher"),
    assembly / mainClass := Some("lv.addresses.BootDispatcher"),
    assemblyMergeStrategy in assembly := {
      case "application.conf" => MergeStrategy.concat
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
  .settings(
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (version.value.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    pomExtra := <url>https://github.com/mrumkovskis/addresses</url>
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
