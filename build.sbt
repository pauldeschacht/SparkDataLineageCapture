name := "SparkDataLineageCapture"
version := "0.1"

// use 2.11.8 in case of Java8
// use 2.11.12 in case of Java9+
scalaVersion := "2.11.8"
val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-encoding",
  "utf8"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test,it"
).map(_.exclude("org.slf4j", "*")) ++ Seq(
  "org.slf4j" % "slf4j-api" % "1.7.30" % "provided"
)

lazy val global = project
  .in(file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "it",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "it",
      "org.apache.hadoop" % "hadoop-minicluster" % "2.7.7" % "it"
    ).map(_.exclude("org.slf4j", "*")) ++ Seq(
      "org.slf4j" % "slf4j-api" % "1.7.30" % "it",
      "org.slf4j" % "jul-to-slf4j" % "1.7.30" % "it",
      "org.slf4j" % "slf4j-log4j12" % "1.7.30" % "it"
    )
  )

parallelExecution in Test := false
