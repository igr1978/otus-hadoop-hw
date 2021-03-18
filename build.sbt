name := "hadoop_hw"

version := "0.1"

scalaVersion := "2.13.5"

// https://mvnrepository.com/artifact/log4j/log4j
libraryDependencies += "log4j" % "log4j" % "1.2.17"
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
//libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.0-alpha1"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.1"