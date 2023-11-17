package org.apache.spark.edu.wpi.dsrg.climber

import java.io.Serializable

import org.apache.spark.SparkContext
import org.apache.spark.edu.wpi.dsrg.climber.cfg.GtCfg
import org.apache.spark.edu.wpi.dsrg.climber.utils.Util.{computeEuDistanceFloat, convert, normalize, printLog}
import org.apache.spark.edu.wpi.dsrg.climber.utils.{Ops, Util}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object GroundTruth extends Serializable {
  def apply(sc: SparkContext): Unit = {
    val gtcfg = GtCfg.getClass(true)
    val gt = new GroundTruth();
    gt.creat_label(sc, gtcfg);
  }
}

class GroundTruth extends Logging with Serializable {
  def creat_label(sc: SparkContext, gtcfg: GtCfg): Unit = {
    val items = if (gtcfg.gt_sample_from_dataset) {
      this.getSample_dataset(sc, gtcfg)
    } else {
      this.getSample_randomWalk(sc, gtcfg)
    }

    val block_paths = Util.getHdfsFileNameList(gtcfg.gt_dataset_path)
    val temp_path = gtcfg.gt_save_path + "-temp/"

    Util.removeIfExist(temp_path)
    Util.removeIfExist(gtcfg.gt_save_path)

    for (idx <- 1 to gtcfg.gt_steps) {
      val start = (block_paths.size * ((idx - 1) * 1.0 / gtcfg.gt_steps)).toInt
      val end = (block_paths.size * (idx * 1.0 / gtcfg.gt_steps)).toInt
      val step_paths = block_paths.slice(start, end)

      sc.objectFile[(Long, Array[Float])](step_paths.mkString(","))
        .flatMap { case (id, elem) => {
          val dists = computeEuDistBatch(elem, items);
          dists.zipWithIndex.map { case (dist, idx) => (idx, dist) }
        }
        }.groupByKey()
        .mapValues(iter => {
          Ops.sort_dists(iter.toSeq, gtcfg.gt_knnK)
        }).saveAsObjectFile(temp_path + idx)

      Util.printLog("==> process %d/%d %d - %d: num: %d".format(idx, gtcfg.gt_steps, start, end, step_paths.size))
    }

    val temp_paths = Util.getHdfsFileNameList(temp_path)
    val holder = ListBuffer.empty[RDD[(Int, Array[Float])]];

    for (path <- temp_paths) {
      val rdd = sc.objectFile[(Int, Array[Float])](path)
      holder += rdd;
    }

    val labelRDD = sc.union(holder.toList)
      .groupByKey()
      .mapValues(iter => {
        val data = iter.toArray.flatten.toSeq
        Ops.sort_dists(data, gtcfg.gt_knnK)
      })

    val dataRdd = sc.parallelize[Array[Float]](items)
      .zipWithIndex()
      .map { case (data, idx) => (idx.toInt, data) }

    dataRdd.join(labelRDD)
      .map(_._2)
      .coalesce(1)
      .saveAsObjectFile(gtcfg.gt_save_path)

    Util.removeIfExist(temp_path)
    printLog("--> save ground truth: %s".format(gtcfg.gt_save_path))
  }

  private def getSample_randomWalk(sc: SparkContext,
                                            gtcfg: GtCfg): Array[Array[Float]] = {
    RandomRDDs.normalVectorRDD(sc, gtcfg.gt_number, gtcfg.gt_length, 1, gtcfg.gt_seed)
      .map(x => convert(x.toArray))
      .map(x => normalize(x))
      .collect()
  }

  private def getSample_dataset(sc: SparkContext,
                                         gtcfg: GtCfg): Array[Array[Float]] = {
    val block_paths = Util.getHdfsFileNameList(gtcfg.gt_dataset_path)
    val path_num = 10
    Random.setSeed(231)
    val samp_block_paths = Random
      .shuffle(block_paths)
      .take(path_num)

    sc.objectFile[(Long, Array[Float])](samp_block_paths.mkString(","))
      .map { case (id, item) => item }
      .sample(false, 0.1, seed = 123)
      .collect()
      .take(gtcfg.gt_number)
  }

  private def computeEuDistBatch(left: Array[Float],
                                          right: Array[Array[Float]]): Array[Float] = {
    val distLB = ArrayBuffer.empty[Float]

    for (right_element <- right) {
      val dist = computeEuDistanceFloat(left, right_element)
      distLB += dist
    }

    distLB.toArray
  }
}