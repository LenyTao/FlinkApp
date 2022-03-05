name := "FlinkApp"

version := "0.1"

scalaVersion := "2.12.12"

val flinkVersion = "1.14.3"


libraryDependencies += "org.apache.flink" %% "flink-scala" % flinkVersion
libraryDependencies += "org.apache.flink" %% "flink-clients" % flinkVersion
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka" % flinkVersion
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % flinkVersion











