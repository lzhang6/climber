package org.apache.spark.edu.wpi.dsrg.climber.utils

import java.io._
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.edu.wpi.dsrg.climber.cfg.{AbsCfg, IdxCfg}
import org.apache.spark.internal.Logging

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.math._
import scala.util.Random

/**
 * Created by leon on 6/26/17.
 * Refacter by Liang on 01/06/2021.
 */
object Util extends Logging {

  def genZero(nbr: Int): String = {
    require(nbr >= 0, s"nbr>=0, but input is $nbr")
    if (nbr == 0) "" else "0" + genZero(nbr - 1)
  }

  def printLog(content: String, isPrint: Boolean = true): Unit = {
    val logCfg = new AbsCfg
    val logFileName = logCfg.logPath

    try {
      val file = new File(logFileName)
      val writer = new PrintWriter(new FileOutputStream(file, true))

      val time = getCurrentTime()

      if (content.contains("Configuration")) {
        writer.append("\n" + "-*" * 15 + time + "-*" * 15 + "\n")
      }

      writer.append(time + content + "\n")
      if (isPrint) {
        println(time + content)
      }
      writer.close()
    } catch {
      case e: java.lang.NullPointerException => logError(e.toString)
    }
  }

  def printError(content: String, isPrint: Boolean = true): Unit = {
    val logCfg = new AbsCfg
    val logFileName = logCfg.logPath + "-error"

    try {
      val file = new File(logFileName)
      val writer = new PrintWriter(new FileOutputStream(file, true))

      val time = getCurrentTime()

      if (content.contains("Configuration")) {
        writer.append("\n" + "-*" * 15 + time + "-*" * 15 + "\n")
      }

      writer.append(time + content + "\n")
      if (isPrint) {
        println(time + content)
      }
      writer.close()
    } catch {
      case e: java.lang.NullPointerException => logError(e.toString)
    }
  }

  private[this] def getCurrentTime(): String = {
    val timeFormat = new SimpleDateFormat("HH:mm:ss - MM.dd.yyyy - z")
    val now = Calendar.getInstance().getTime()
    timeFormat.format(now)
  }

  def stddev_int(xs: Iterable[Int]): Double = {
    val input = xs.toList.map(_.toDouble)
    val avg = this.meanDouble(input)
    val std = this.stddevDouble(input, avg)
    std
  }


  def meanDouble(xs: List[Double]): Double = xs match {
    case Nil => 0.0
    case ys => ys.reduceLeft(_ + _) / ys.size.toDouble
  }

  def stddevDouble(xs: List[Double], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) {
      (a, e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }

  def euclideanDistance(xs: Array[Float], ys: Array[Float]): Double = {
    sqrt((xs zip ys).map { case (x, y) => pow(y - x, 2) }.sum)
  }

  def readConfigFile(configPath: String): Map[String, String] = {
    var staff: Array[String] = null
    try {
      val configFile = Source.fromFile(configPath)
      staff = configFile.getLines().toArray
      configFile.close()
    } catch {
      case e1: FileNotFoundException => logError(configPath + e1.toString)
      case e2: IOException => logError(configPath + e2.toString)
        System.exit(0)
    }

    var content = HashMap.empty[String, String]
    val sep = "="
    for (line <- staff if (line.contains(sep) && !line.contains("#"))) {
      try {
        val t = line.split(sep)
        content += (t(0).trim -> t(1).trim)
      } catch {
        case e: Exception => println("==> %s can't parse %s".format(line.toString, e.toString))
      }
    }

    content.toMap
  }

  def checkDir(file: File): Unit = {
    val dir = new File(file.getParent)
    if (!dir.exists()) {
      dir.mkdir()
    }
  }

  def removeIfExist(path: String): Unit = {
    if (this.hdfsDirExists(path)) {
      this.removeHdfsFiles(path)
    }
  }

  def hdfsDirExists(hdfsDirectory: String): Boolean = {
    val fs = FileSystem.get(new URI(hdfsDirectory), new Configuration())
    try {
      fs.exists(new Path(hdfsDirectory))
    } catch {
      case e: java.net.ConnectException => {
        logError(e.toString)
        false
      }
    }
  }

  def removeHdfsFiles(hdfsDirectory: String): Unit = {
    val fs = FileSystem.get(new URI(hdfsDirectory), new Configuration())
    try {
      fs.delete(new Path(hdfsDirectory), true)
    } catch {
      case e: Exception => logError(e.toString)
    }

    printLog("==> remove %s".format(hdfsDirectory), true)
  }

  def getHdfsFileNameList(hdfsFileName: String): List[String] = {
    val fs = FileSystem.get(new URI(hdfsFileName), new Configuration())
    fs.listStatus(new Path(hdfsFileName)).map {
      _.getPath.toString
    }.filter(x => !x.contains("_SUCCESS")).toList
  }

  def writeArrayBytesToHdfs(hdfsFileName: String, data: Array[Byte]): Unit = {
    val fs = FileSystem.get(new URI(hdfsFileName), new Configuration())

    if (Util.hdfsDirExists(hdfsFileName))
      Util.removeHdfsFiles(hdfsFileName)

    val os = fs.create(new Path(hdfsFileName))
    try {
      os.write(data)
    } finally {
      os.close()
    }
  }

  def getHdfsFileSizeMList(hdfsDirectory: String): List[Double] = {
    val hadoopConf = new Configuration()
    val uri = new URI(hdfsDirectory)
    val fListStatus = FileSystem.get(uri, hadoopConf).listStatus(new Path(hdfsDirectory))
    fListStatus.map { x => convertToM(x.getLen) }.filter(x => x != 0.0).toList
  }

  def get_sample_block_paths(pmtcfg: IdxCfg): String = {
    val block_paths = Util.getHdfsFileNameList(pmtcfg.dataset_path)
    val path_num = math.ceil(block_paths.size * pmtcfg.percent).toInt

    printLog("==> sampling block num: %d".format(path_num))

    val samp_block_paths = Random
      .shuffle(block_paths)
      .take(path_num)
    samp_block_paths.mkString(",")
  }

  def getArrayBytesFromHdfs(hdfsDirectory: String): Array[Byte] = {
    require(hdfsDirExists(hdfsDirectory), "%s doesn't exist".format(hdfsDirectory))

    val fs = FileSystem.get(new URI(hdfsDirectory), new Configuration())
    val path = new Path(hdfsDirectory)
    val is = fs.open(path)
    val fileSize = fs.listStatus(path).map {
      _.getLen
    }.max.toInt

    var contentBuffer = new Array[Byte](fileSize)
    is.readFully(0, contentBuffer)
    require((fileSize == contentBuffer.size), "(fileSize: %d == contentBuffer.size: %d)".format(fileSize,
      contentBuffer.size))
    is.close()
    contentBuffer
  }

  def fetchBoolFromString(input: String): Boolean = {
    val content = input.trim.toLowerCase
    if (content == "true" || content == "yes") true
    else false
  }

  private def convertToM(size: Long): Double = {
    (size * 1.0) / (1024 * 1024)
  }

  def convert(ts: Array[Double]): Array[Float] = {
    val tn = ts.map(x => x.toFloat)
    for (i <- 1 until tn.length) {
      tn(i) = tn(i - 1) + tn(i)
    }
    tn
  }

  def normalize(ts: Array[Float]): Array[Float] = {
    val count = ts.length
    val mean = ts.sum / count
    val variance = ts.map(x => math.pow(x - mean, 2)).sum / (count - 1)
    val std = math.sqrt(variance)
    ts.map(x => ((x - mean) / std).toFloat)
  }

  def computeEuDistanceFloat(left: Array[Float], right: Array[Float]): Float = {
    val value = (left zip right).map { case (x, y) => pow(y - x, 2) }.sum
    pow(value, 0.5).toFloat
  }

  def compute_spearman_rho(query_pivot_order_map: Map[Short, Int], p4s_os_str: String): Int = {
    val p4s_os = p4s_os_str.split(":").map(_.toShort)

    var spearman_rho_dist = 0
    for ((pivot, pos) <- p4s_os.zipWithIndex) {
      val pivot_pos = query_pivot_order_map.getOrElse(pivot, query_pivot_order_map.size)
      //      println(pivot_pos - (pos + 1), pivot)
      spearman_rho_dist += (pivot_pos - (pos + 1)) * (pivot_pos - (pos + 1))
    }

    spearman_rho_dist
  }

  def compute_overlap_dist(left: Array[Short], right: Array[Short]): Int = {
    var set = scala.collection.mutable.Set[Short]()
    set ++= left
    set ++= right

    left.length - (2 * left.length - set.size)
  }

  def compute_kendall_tau(query_pp: Array[Short], pp: Array[Short]): Float = {
    val p1 = query_pp
    val h2 = pp.zipWithIndex.toMap

    val prefix_len = p1.length
    var sum = 0
    for (a_idx <- 0 until prefix_len) {
      for (b_idx <- a_idx + 1 until prefix_len) {
        val a_1 = h2.getOrElse(p1(a_idx), -1)
        val a_2 = h2.getOrElse(p1(b_idx), -1)
        if (a_1 != -1 && a_2 != -1 && a_1 < a_2) {
          sum += 0
        } else {
          sum += 1
        }
      }
    }

    //    println("sum\t",sum)
    sum * 2.0F / (prefix_len * (prefix_len - 1))
  }

  def compute_kendall_tau(query_pp: Array[Short], pp: Array[Short], prefix_len: Int): Float = {
    val p1 = query_pp.take(prefix_len)
    val h2 = pp.take(prefix_len).zipWithIndex.toMap

    var sum = 0
    for (a_idx <- 0 until prefix_len) {
      for (b_idx <- a_idx + 1 until prefix_len) {
        val a_1 = h2.getOrElse(p1(a_idx), -1)
        val a_2 = h2.getOrElse(p1(b_idx), -1)
        if (a_1 != -1 && a_2 != -1 && a_1 < a_2) {
          sum += 0
        } else {
          sum += 1
        }
      }
    }

    //    println("sum\t",sum)
    sum * 2.0F / (prefix_len * (prefix_len - 1))
  }

  def compute_kendall_tau2(query_pp: Array[Short], pp: Array[Short], prefix_len: Int): Float = {
    val p1 = query_pp.take(prefix_len)
    val h2 = pp.zipWithIndex.toMap

    var sum = 0
    for (a_idx <- 0 until prefix_len) {
      for (b_idx <- a_idx + 1 until prefix_len) {
        val a_1 = h2.getOrElse(p1(a_idx), -1)
        val a_2 = h2.getOrElse(p1(b_idx), -1)
        if (a_1 != -1 && a_2 != -1 && a_1 < a_2) {
          sum += 0
        } else {
          sum += 1
        }
      }
    }

    //    println("sum\t",sum)
    sum * 2.0F / (prefix_len * (prefix_len - 1))
  }
}