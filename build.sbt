ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

val AkkaVersion = "2.7.0"
libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "5.0.0",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion
)
lazy val root = (project in file("."))
  .settings(
    name := "Sensor-stats"
  )
