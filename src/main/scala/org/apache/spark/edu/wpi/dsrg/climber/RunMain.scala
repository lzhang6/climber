package org.apache.spark.edu.wpi.dsrg.climber

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object RunMain extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = initializeSparkContext()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    args(0) match {
      case "-g" => RandWalkTsCreater(sc)
      case "-c" => GroundTruth(sc)
      case "-b" => Index(sc)
      case "-q" => Query(sc)
      case "-d" => Debug(sc, args(1))
      case _ => printHelpMsg()
    }
    spark.stop()
  }

  def printHelpMsg(): Unit = {
    println("Usage: configuration file is ./etc/config.conf")
    println("Generate data:  -g")
    println("Build index  :  -b")
    println("Create labels:  -c")
    println("Query process:  -q")
    println("help -h")
  }

  private[climber] def initializeSparkContext(): SparkSession = {
    val conf = new SparkConf().setAppName("Climber")
    conf.set("spark.sql.parquet.compression.codec", "uncompressed")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark
  }
}
