package org.apache.spark.edu.wpi.dsrg.climber.utils

import org.apache.spark.SparkContext
import org.apache.spark.edu.wpi.dsrg.climber.cfg.IdxCfg

import scala.collection.mutable


object Statistic extends Serializable {
  def apply(path: String): Statistic = new Statistic(path)
}

class Statistic() extends Serializable {
  var hdfsFileSizeMList: List[Double] = _

  var partitionNbr: Long = _
  var blockNbr: Long = _

  var tsFileSizeG: Double = _
  var diskSizeG: Double = _

  var maxValue: Double = _
  var minValue: Double = _
  var meanValue: Double = _
  var stddevValue: Double = _

  def this(hdfsFilePath: String) {
    this()
    this.hdfsFileSizeMList = Util.getHdfsFileSizeMList(hdfsFilePath)
  }

  def fetchAll(): String = {
    partitionNbr = hdfsFileSizeMList.size
    tsFileSizeG = hdfsFileSizeMList.sum / 1024.0
    maxValue = hdfsFileSizeMList.max
    minValue = hdfsFileSizeMList.min
    meanValue = Util.meanDouble(hdfsFileSizeMList)
    stddevValue = Util.stddevDouble(hdfsFileSizeMList, meanValue)

    ("\n * partition num:      \t%d" +
      "\n * data size           \t%.2f G" +
      "\n * max                 \t%.2f M" +
      "\n * min                 \t%.2f M" +
      "\n * mean                \t%.2f M" +
      "\n * stddev              \t%.2f M").format(
      partitionNbr,
      tsFileSizeG,
      maxValue,
      minValue,
      meanValue,
      stddevValue)
  }

  def fetchSizeM(): String = {
    "\n * size\t%.2f M".format(hdfsFileSizeMList.sum)
  }

  def histogram(sc: SparkContext, pmtcfg: IdxCfg): String = {
    val th = pmtcfg.block_size
    val bps = Array(0.25, 0.5, 0.75, 1.0, 1.5, 2.0, Double.PositiveInfinity).map {
      _ * th
    }

    def convert(v: Double): Int = bps.indexWhere(v < _)

    val keyNbr = sc.parallelize(hdfsFileSizeMList)
      .map(x => (convert(x), 1))
      .reduceByKey((a, b) => a + b)
      .collect().toList

    val keyNbrMap = mutable.HashMap.empty[Int, Int]
    keyNbrMap ++= keyNbr
    val result = keyNbrMap.toMap

    var output = ""

    for ((v, idx) <- bps.zipWithIndex) {
      val start = if (idx == 0) 0.0 else bps(idx - 1)
      val nbr = try {
        result(idx)
      } catch {
        case e: Exception => 0
      }
      val content = if (idx == bps.length - 1) {
        "\n * %3d <-> Max  : %d".format(start.toInt, nbr)
      } else {
        "\n * %3d <-> %3dM : %d".format(start.toInt, bps(idx).toInt, nbr)
      }
      output += content
    }
    output
  }
}
