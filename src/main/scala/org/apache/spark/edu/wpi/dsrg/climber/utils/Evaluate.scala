package org.apache.spark.edu.wpi.dsrg.climber.utils

import breeze.linalg.min
import org.apache.spark.edu.wpi.dsrg.climber.utils.Util.printLog
import org.apache.spark.internal.Logging
import scala.Numeric.Implicits._

object Evaluate extends Logging {
  def knn(result: List[(Array[Float], Array[Float])],
          size_list: List[(Int, Int)],
          duration: Long): Unit = {
    val recall_input = result.map { case (query_pred, ground_truth) => callRecall(query_pred, ground_truth) }
    val errors_input = result.map { case (query_pred, ground_truth) => calErrorRatio(query_pred, ground_truth) }
    val recall = Util.meanDouble(recall_input)
    val errorRatio = Util.meanDouble(errors_input)

    val recall_std = stdDev(recall_input)

    val avg_candidate = Util.meanDouble(size_list.map(_._1.toDouble))
    val avg_partition_size = Util.meanDouble(size_list.map(_._2.toDouble))

    val content =
      ("\n==> Result: kNN query" +
        "\n * recall          \t%.3f" +

        "\n * recall_std      \t%.3f" +
        "\n * errorRatio      \t%.3f" +
        "\n * candidate       \t%.0f" +

        "\n * partition       \t%.2f" +
        "\n * avg time        \t%d ms, %d s"
        ).format(
        recall,
        recall_std,
        errorRatio,
        avg_candidate,
        avg_partition_size,
        duration / result.size, duration / (result.size * 1000)
      )

    printLog(content)
  }

  def calAveragePrecision(pred: Array[Float], ground_truth: Array[Float]): Double = {
    require(ground_truth.sum < pred.sum, "switch input of pred and ground_truth")

    if (ground_truth.nonEmpty) {
      var prior = 0
      var sum_precsion = 0.0

      for ((v_pred, idx) <- pred.zipWithIndex) {
        if (ground_truth.contains(v_pred)) {
          prior += 1
          sum_precsion += prior * 1.0 / (idx + 1)
        }
      }
      sum_precsion / ground_truth.length

    } else {
      logWarning("Empty ground truth set, check input data")
      0.0
    }
  }

  def calErrorRatio(values: Array[Float], gTruth: Array[Float]): Double = {
    val values_sort = values.sortWith(_ < _)
    val gTruth_sort = gTruth.sortWith(_ < _)

    val length = min(values_sort.length, gTruth_sort.length)
    val result = new Array[Double](length)

    for (i <- 0 until length) {
      if (gTruth_sort(i) == 0)
        result(i) = 1.0
      else {
        result(i) = values_sort(i) * 1.0 / gTruth_sort(i)
      }
    }
    Util.meanDouble(result.toList)
  }

  def callRecall(listC: Array[Float], listG: Array[Float]): Double = {
    val leftSort = listC.sortWith(_ < _)
    val rightSort = listG.sortWith(_ < _)

    val len = leftSort.length
    var sameValue = 0
    var leftIdx = 0
    var rightIdx = 0

    do {
      if (leftSort(leftIdx) == rightSort(rightIdx)) {
        sameValue += 1
        leftIdx += 1
        rightIdx += 1
      } else if (leftSort(leftIdx) > rightSort(rightIdx)) {
        rightIdx += 1
      } else {
        leftIdx += 1
      }
    } while (leftIdx < len && rightIdx < len)

    sameValue.toDouble / len
  }

  private[climber] def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  private[climber] def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  private[climber] def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))
}