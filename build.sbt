name := "SparkDataLineageCapture"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.scalatest" %% "scalatest" % "3.1.0" % "test"
)
