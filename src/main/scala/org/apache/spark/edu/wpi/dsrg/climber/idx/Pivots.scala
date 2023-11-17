package org.apache.spark.edu.wpi.dsrg.climber.idx

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.edu.wpi.dsrg.climber.cfg.IdxCfg
import org.apache.spark.edu.wpi.dsrg.climber.utils.Util.printLog
import org.apache.spark.edu.wpi.dsrg.climber.utils.{Ops, Util}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting.quickSort


object Pivots extends Logging with Serializable {
  def get_pivot_bc(sc: SparkContext,
                   idxcfg: IdxCfg
                  ): Broadcast[Array[(Short, Array[Float])]] = {
    if (Util.hdfsDirExists(idxcfg.pivot_path)) {
      read_pivots_bc(sc, idxcfg)
    } else {
      this.generate_save_pivots(sc, idxcfg)
    }
  }

  def generate_save_pivots(sc: SparkContext,
                           idxcfg: IdxCfg
                          ): Broadcast[Array[(Short, Array[Float])]] = {
    printLog("==> construct pivots (paa format)")
    val pivots = sc.objectFile[(Short, Array[Float])](idxcfg.pivot_path_raw)
      .map{case (pid, pivots_ds) => (pid, Ops.cvtTsToPaa(pivots_ds, idxcfg.paa_length))}
      .collect()

    Util.removeIfExist(idxcfg.pivot_path)
    sc.parallelize(pivots).coalesce(1).saveAsObjectFile(idxcfg.pivot_path)
    printLog("==> save pivots (paa) to %s".format(idxcfg.pivot_path))

    val pivots_bc = sc.broadcast(pivots)
    pivots_bc
  }

//  def random_select(paa_rdd_cache: RDD[Array[Float]],
//                    idxcfg: IdxCfg): Array[(Short, Array[Float])] = {
//    val pivots = paa_rdd_cache
//      .takeSample(withReplacement = false, idxcfg.pivot_num)
//      .zipWithIndex
//      .map { case (pivot, id) => ((id + 1).toShort, pivot) }
//
//    pivots
//  }

  def balanced_pivot_position(sc: SparkContext,
                              paa_rdd_cache: RDD[Array[Float]],
                              idxcfg: IdxCfg): Array[(Short, Array[Float])] = {
    val iter_num = 5
    val sample_percent = 0.5
    val pivot_bf = ArrayBuffer.empty[Array[Float]]

    val rcd_num = paa_rdd_cache.count() * sample_percent
    println("==> expect mean %d".format((rcd_num / idxcfg.pivot_num).toInt))

    for (iter <- 1 to iter_num) {
      val pid_pivot = paa_rdd_cache
        .takeSample(withReplacement = false, idxcfg.pivot_num)
        .zipWithIndex.map { case (pivot, id) => ((id + 1).toShort, pivot) }

      val pid_std = this.compute_std(paa_rdd_cache
        .sample(false, sample_percent), pid_pivot)

      val (std_mean, std_std) = this.evaluate(pid_std)
      Util.printLog("==> pivot selection %d/%d std_mean: %.2f, std_std:%.2f".format(iter, iter_num, std_mean, std_std))

      val std_pivot = pid_std.map { case (pid, std) => (std, pid) }
      quickSort(std_pivot)

      val pids_cur = std_pivot.map(_._2).take(idxcfg.pivot_num / iter_num)
      val pids_hs = HashSet(pids_cur: _*)
      val pivots_batch = pid_pivot.filter { case (pid, pivot) => pids_hs.contains(pid) }.map(_._2)

      //      println("pids_cur", pids_cur.mkString(","))
      //      println("hashset ",pids_hs.toArray.mkString(","))
      //      println("pivots_batch ",pivots_batch.length)
      //      pivots_batch.take(5).foreach(x => println(x.take(5).mkString(":")))

      pivot_bf ++= pivots_batch
    }

    val pivots_final = pivot_bf.toArray.zipWithIndex.map { case (pivot, id) => ((id + 1).toShort, pivot) }

    val final_pid_std = this.compute_std(paa_rdd_cache, pivots_final)
    val (final_std_mean, final_std_std) = this.evaluate(final_pid_std)
    Util.printLog("==> Final: std_mean: %.2f, std_std:%.2f".format(final_std_mean, final_std_std))

    pivots_final.foreach(x => println(x._1, x._2.take(5).mkString(":")))
    pivots_final
  }

  private[climber] def compute_std(paa_rdd_cache: RDD[Array[Float]],
                                pivots: Array[(Short, Array[Float])]): Array[(Short, Double)] = {
    val pid_std = paa_rdd_cache
      .flatMap(paa => {
        val permutation = Ops.cvtPaaToPermutation(paa, pivots)
        permutation.map { case (pid, pos) => (pid.toString + ":" + pos.toString, 1) }
      }).reduceByKey((a, b) => a + b)
      .map { case (pid_pos, num) => (pid_pos.split(":").head.toShort, num) }
      .groupByKey()
      .map { case (pid, iter) => (pid, Util.stddev_int(iter)) }
      .collect()

    pid_std
  }

  private[climber] def evaluate(pid_std: Array[(Short, Double)]): (Double, Double) = {
    val std_value = pid_std.map(_._2)
    val std_mean = Util.meanDouble(std_value.toList)
    val std_std = Util.stddevDouble(std_value.toList, std_mean)
    (std_mean, std_std)
  }

  def read_pivots_bc(sc: SparkContext,
                     idxcfg: IdxCfg
                    ): Broadcast[Array[(Short, Array[Float])]] = {
    if (Util.hdfsDirExists(idxcfg.pivot_path)) {
      printLog("==> read pivots from disk")
    } else {
      printLog("==> pivot not exist: %s".format(idxcfg.pivot_path))
    }
    val pivots = sc.objectFile[(Short, Array[Float])](idxcfg.pivot_path).collect()
    sc.broadcast(pivots)
  }
}