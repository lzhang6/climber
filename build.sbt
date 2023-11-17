name := "climber"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "org.apache.spark"  % "spark-mllib_2.11" % "2.1.1" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0" % "runtime"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP4" % Test

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % "2.11.8",
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "org.scala-lang.modules" % "scala-parser-combinators_2.11" % "1.0.4",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4",
  "org.scala-lang" % "scala-library" % "2.11.8"
)