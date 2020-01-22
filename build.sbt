name := "upskill"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

val geoSparkVersion = "1.1.3"

libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,

  // GeoSpark
  "org.datasyslab" % "geospark" % geoSparkVersion,
  "org.datasyslab" % "geospark-sql_2.2" % geoSparkVersion,

  // Test
  "junit" % "junit" % "4.10" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,

  // Scala Typesafe library
  "com.github.andyglow" %% "typesafe-config-scala" % "1.0.1"
)