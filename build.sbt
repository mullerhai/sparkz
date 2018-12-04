name := "sparkz"

version := "0.1"

scalaVersion := "2.12.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.2"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"

