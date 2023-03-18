ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"
val sparkVersion = "2.4.1"
val kafkaVersion = "3.1.0"
lazy val root = (project in file("."))
  .settings(
    name := "SparkDeveloperHomeWork7_2_12",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
    ),
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.4.2",
      "org.apache.kafka" % "kafka-streams" % kafkaVersion,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
    ),
    // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
    libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.8",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5",
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
    dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.5",

  )

