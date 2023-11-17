package org.apache.spark.edu.wpi.dsrg.climber

import java.io.Serializable

import org.apache.spark.edu.wpi.dsrg.climber.cfg.{DbgCfg, IdxCfg}
import org.apache.spark.edu.wpi.dsrg.climber.idx.Pivots
import org.apache.spark.edu.wpi.dsrg.climber.utils.Evaluate.callRecall
import org.apache.spark.edu.wpi.dsrg.climber.utils.{Evaluate, Ops, Util}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext}

import scala.collection.immutable.HashSet
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random
import scala.util.control.Breaks._

object Debug extends Serializable {
  def apply(sc: SparkContext, runType: String): Unit = {
    val idxcfg = IdxCfg.getClass(true)
    val debug = new Debug
    runType match {
      case "u" => debug.update_pivots(sc, idxcfg)
      case "c" => debug.construct_paa(sc, idxcfg.dataset_path)
      case "q" => debug.query_paa(sc, idxcfg)
    }
  }
}

class Debug extends Logging with Serializable {
  def construct_paa(sc: SparkContext, data_path: String): Unit = {
    val dbgCfg = DbgCfg.get_class()
    val paas = dbgCfg.paas
    val paa_cached = sc.objectFile[(Long, Array[Float])](data_path).map { case (rid, ts) =>
      val paa_bf = ArrayBuffer.empty[Array[Float]]
      for (paa <- paas) {
        paa_bf.append(Ops.cvtTsToPaa(ts, paa))
      }
      val pid = TaskContext.getPartitionId()
      (pid, rid, paa_bf.toArray)
    }.cache()

    for ((paa, idx) <- paas.zipWithIndex) {
      val save_path = data_path + "-debug/paa_" + paa.toString
      paa_cached.map { case (pid, rid, paa_bf) => (pid, rid, paa_bf(idx)) }.saveAsObjectFile(save_path)
      Util.printLog("=> save to " + save_path, true)
    }
  }

  def query_paa(sc: SparkContext, idxcfg: IdxCfg): Unit = {
    val dbgCfg = DbgCfg.get_class()
    val ground_truth = sc.objectFile[(Array[Float], Array[Float])](idxcfg.label_path)
      .map { case (query, label) => (query, label.take(idxcfg.knn_K)) }
      .take(idxcfg.query_num)
    Util.printLog("==> read ground truth from disk")


    for (paa <- dbgCfg.paas) {
      breakable {
        for (cand <- dbgCfg.cands) {
          Util.printLog("==> start query PAA " + paa.toString + " cands " + cand.toString)
          val cur_mean_recall = this.query_one_paa(sc, idxcfg, paa, cand, ground_truth)
          if (cur_mean_recall == 1.0) {
            Util.printLog("==> BREAK")
            break()
          }
        }
      }
    }
  }

  def query_one_paa(sc: SparkContext,
                    idxcfg: IdxCfg,
                    paa: Int,
                    cand: Int,
                    gt: Array[(Array[Float], Array[Float])]): Double = {
    val paa_path = idxcfg.dataset_path + "-debug/paa_" + paa.toString

    val pred_gt_combine = ListBuffer.empty[(Array[Float], Array[Float])]

    val raw_data = sc.objectFile[(Long, Array[Float])](idxcfg.dataset_path).cache()

    for (((query, dist_label), idx) <- gt.zipWithIndex) {
      val query_paa = Ops.cvtTsToPaa(query, paa)
      val pid_rids_cache = sc.objectFile[(Int, Long, Array[Float])](paa_path)
        .map { case (pid, rid, paa) => (pid, rid, Util.computeEuDistanceFloat(paa, query_paa))
        }.cache()

      val threshold = pid_rids_cache.sortBy(_._3).take(cand).last._3

      val pid_rids = pid_rids_cache
        .filter { case (pid, rid, dist) => dist <= threshold }
        .map { case (pid, rid, dist) => pid.toString + "-" + rid.toString }.collect()
      val hash_set = HashSet(pid_rids: _*)
      val hs_bc = sc.broadcast(hash_set)

      val dist_pred = raw_data.filter { case (rid, ts) =>
        val pid_rid = TaskContext.getPartitionId().toString + "-" + rid.toString
        hs_bc.value.contains(pid_rid)
      }.map {
        case (rid, ts) => Util.computeEuDistanceFloat(query, ts)
      }.takeOrdered(idxcfg.knn_K)

      val recall = Evaluate.callRecall(dist_pred, dist_label)
      Util.printLog("\t --> %d / %d, recall: %.2f".format(idx + 1, gt.length, recall))
      pred_gt_combine += ((dist_pred, dist_label))
    }

    val recall_input = pred_gt_combine.toArray.map { case (query_pred, ground_truth) => callRecall(query_pred,
      ground_truth)
    }
    val mean_recall = Util.meanDouble(recall_input.toList)
    Util.printLog("==> Result, paa: %d, cand: %d, recall %.2f".format(paa, cand, mean_recall))

    mean_recall
  }

  def construct_rank_noorder(sc: SparkContext, idxcfg: IdxCfg): Unit = {
    val dbgCfg = DbgCfg.get_class()
    val pivots = this.read_pivots(sc, idxcfg)
    val pivots_bc = sc.broadcast(pivots)

    val pp_cache = sc.objectFile[(Long, Array[Float])](idxcfg.dataset_path)
      .map { case (rid, ds) =>
        //        val pid = TaskContext.getPartitionId()
        val pp_bf = ArrayBuffer.empty[Array[Short]]
        val p4s_os_whole = Ops.cvtPaaTo4Ps_os_whole(ds, pivots_bc.value)

        for (pp <- dbgCfg.pps) {
          val p4s_oi = p4s_os_whole.take(pp)
          pp_bf.append(p4s_oi)
        }
        //        println("--> process\t rid: %d".format(rid))
        (rid, pp_bf.toArray)
      }.cache()

    for ((pp, idx) <- dbgCfg.pps.zipWithIndex) {
      val save_path = idxcfg.dataset_path + "-debug/rank_" + idxcfg.pivot_num.toString + "_" + pp.toString
      pp_cache.map { case (rid, pp_bf) => (rid, pp_bf(idx)) }.saveAsObjectFile(save_path)
      Util.printLog("=> save to " + save_path, true)
    }
  }

  def query_rank_noorder(sc: SparkContext, idxcfg: IdxCfg): Unit = {
    val dbgCfg = DbgCfg.get_class()
    val gt = sc.objectFile[(Array[Float], Array[Float])](idxcfg.label_path)
      .map { case (query, label) => (query, label.take(idxcfg.knn_K)) }
      .take(idxcfg.query_num)
    Util.printLog("==> read ground truth from disk")

    val pivots = this.read_pivots(sc, idxcfg)

    for (pp <- dbgCfg.pps) {
      breakable {
        for (cand <- dbgCfg.cands) {
          Util.printLog("==> start query PP_lenth " + pp.toString + " cands " + cand.toString)
          val cur_mean_recall = this.query_one_rank_noorder(sc, idxcfg, pivots, pp, cand, gt)
          if (cur_mean_recall == 1.0) {
            Util.printLog("==> BREAK")
            break()
          }
        }
      }
    }
  }

  def query_one_rank_noorder(sc: SparkContext,
                             idxcfg: IdxCfg,
                             pivots: Array[(Short, Array[Float])],
                             pp: Int,
                             cand: Int,
                             gt: Array[(Array[Float], Array[Float])]
                            ): Double = {
    val pivots_bc = sc.broadcast(pivots)
    val rank_os_path = idxcfg.dataset_path + "-debug/rank_" + idxcfg.pivot_num.toString + "_" + pp.toString
    val pred_gt_combine = ListBuffer.empty[(Array[Float], Array[Float])]

    val raw_data = sc.objectFile[(Long, Array[Float])](idxcfg.dataset_path).cache()
    val raw_rank = sc.objectFile[(Long, Array[Short])](rank_os_path).cache

    val rcd_nums = ArrayBuffer.empty[Int]

    for (((query, dist_label), idx) <- gt.zipWithIndex) {
      val query_p4s_oi = Ops.cvtPaaTo4Ps_os(query, pivots_bc.value, pp)
      val pid_rids_cache = raw_rank
        .map { case (rid, p4s_oi) => (rid, Util.compute_overlap_dist(query_p4s_oi, p4s_oi))
        }.cache()

      val threshold = pid_rids_cache.sortBy(_._2).take(cand).last._2

      val rids = pid_rids_cache
        .filter { case (rid, dist) => dist <= threshold }
        .map { case (rid, dist) => rid }.collect()
      rcd_nums.append(rids.length)
      val hash_set = HashSet(rids: _*)
      val hs_bc = sc.broadcast(hash_set)

      val dist_pred = raw_data.filter { case (rid, ts) =>
        hs_bc.value.contains(rid)
      }.map {
        case (rid, ts) => Util.computeEuDistanceFloat(query, ts)
      }.takeOrdered(idxcfg.knn_K)

      val recall = Evaluate.callRecall(dist_pred, dist_label)
      Util.printLog("\t --> %d / %d, recall: %.2f, rcd_num: %d/%d, threshold: %d".format(idx + 1, gt.length, recall,
        rids.length, cand, threshold))
      pred_gt_combine += ((dist_pred, dist_label))
    }

    val recall_input = pred_gt_combine.toArray.map { case (query_pred, ground_truth) => callRecall(query_pred,
      ground_truth)
    }
    val mean_recall = Util.meanDouble(recall_input.toList)
    val mean_rcd_num = Util.meanDouble(rcd_nums.toList.map(_.toDouble))
    Util.printLog("==> Result, pp: %d, cand: %d, rcd_num: %.2f, recall %.2f".format(pp, cand, mean_rcd_num,
      mean_recall))

    mean_recall
  }

  def read_pivots(sc: SparkContext, idxcfg: IdxCfg): Array[(Short, Array[Float])] = {
    val pivots_path = idxcfg.dataset_path + "-debug/pivots_" + idxcfg.pivot_num.toString

    if (!Util.hdfsDirExists(pivots_path)) {
      Util.printLog("==> build pivots ...")
      val block_paths = Util.getHdfsFileNameList(idxcfg.dataset_path)
      val block_num = 10
      Random.setSeed(231)
      val samp_block_paths = Random
        .shuffle(block_paths)
        .take(block_num)

      val pivots = sc.objectFile[(Long, Array[Float])](samp_block_paths.mkString(","))
        .sample(false, 0.1, seed = 123).zipWithIndex()
        .filter { case ((rid, ds), id) => id != 0 && id <= idxcfg.pivot_num }
        .map { case ((rid, ds), id) => (id.toShort, ds) }
        .collect()

      sc.parallelize(pivots).saveAsObjectFile(pivots_path)
      Util.printLog("==> save pivots %s".format(pivots_path))
    }

    Util.printLog("==> read pivots %s".format(pivots_path))
    sc.objectFile[(Short, Array[Float])](pivots_path).collect()
  }

  def construct(sc: SparkContext, idxcfg: IdxCfg): Unit = {
    val paa_path = idxcfg.dataset_path + "-output/paa-" + idxcfg.paa_length
    val paa_rdd_cache = if (Util.hdfsDirExists(paa_path)) {
      sc.objectFile[Array[Float]](paa_path).cache()
    } else {
      sc.objectFile[(Long, Array[Float])](idxcfg.dataset_path)
        .map { case (id, ts) => Ops.cvtTsToPaa(ts, idxcfg.paa_length) }
        .cache()
    }

    val pivots_bc = Pivots.get_pivot_bc(sc, idxcfg)
    paa_rdd_cache.saveAsObjectFile(paa_path)

    paa_rdd_cache
      .map { paa => {
        val p4s_os = Ops.cvtPaaTo4Ps_os(paa, pivots_bc.value, idxcfg.pp_length)
        val p4s_os_str = p4s_os.mkString(":")
        (p4s_os_str, 1L)
      }
      }.reduceByKey((a, b) => a + b)
      .saveAsObjectFile(idxcfg.dataset_path + "-output/p4s-" + idxcfg.pivot_num)
  }

  def update_pivots(sc: SparkContext, idxcfg: IdxCfg): Unit = {
    val pivots = sc.objectFile[(Short, Array[Float])](idxcfg.pivot_path).cache()

    for (seg_len <- Array(25)) {
      val save_path = idxcfg.pivot_path + "-" + (50 * seg_len).toString
      pivots.map { case (pid, paa) => (pid, Ops.cvtTsToPaa(paa, seg_len)) }
        .coalesce(1).saveAsObjectFile(save_path)
      Util.printLog("--> save to %s".format(save_path))
    }
  }

  def construct_full_pivots(sc: SparkContext, idxcfg: IdxCfg): Unit = {
    val pivots = this.read_pivots(sc, idxcfg)
    val pivots_bc = sc.broadcast(pivots)
    val save_path = idxcfg.dataset_path + "-debug/idx_full_" + idxcfg.pivot_num.toString

    sc.objectFile[(Long, Array[Float])](idxcfg.dataset_path)
      .map { case (rid, ds) =>
        val p4s_os_whole = Ops.cvtPaaTo4Ps_os_whole(ds, pivots_bc.value)
        (rid, p4s_os_whole)
      }.saveAsObjectFile(save_path)

    Util.printLog("=> save to " + save_path, true)
  }

  def query_full_pivots(sc: SparkContext,
                        idxcfg: IdxCfg,
                        prefix_len_f1: Int,
                        prefix_len_f2: Int,
                        cand: Int): Unit = {
    val f1_time = 1

    val gt = sc.objectFile[(Array[Float], Array[Float])](idxcfg.label_path)
      .map { case (query, label) => (query, label.take(idxcfg.knn_K)) }
      .take(idxcfg.query_num)
    Util.printLog("==> read ground truth from disk")
    val pivots = this.read_pivots(sc, idxcfg)

    val pred_gt_combine_f1 = ListBuffer.empty[(Array[Float], Array[Float])]
    val pred_gt_combine_f2 = ListBuffer.empty[(Array[Float], Array[Float])]

    val raw_data = sc.objectFile[(Long, Array[Float])](idxcfg.dataset_path).cache()
    val raw_idx = sc.objectFile[(Long, Array[Short])](idxcfg.dataset_query_path).cache

    val f1_nums = ArrayBuffer.empty[Int]
    val f2_nums = ArrayBuffer.empty[Int]

    for (((query, dist_label), idx) <- gt.zipWithIndex) {
      val query_pp = Ops.cvtPaaTo4Ps_os_whole(query, pivots)

      val pid_rids_cache = raw_idx.map {
        case (rid, pp) => (rid, pp, Util.compute_overlap_dist(query_pp.take(prefix_len_f1), pp.take(prefix_len_f1)))
      }.cache()

      val threshold = pid_rids_cache.sortBy(_._3).take(cand * f1_time).last._3

      val pid_rids_cache2 = pid_rids_cache
        .filter { case (rid, pp, dist) => dist <= threshold }
        .map { case (rid, pp, dist) => (rid, pp) }.cache()

      val rids_f1 = pid_rids_cache2.map(_._1).collect()

      val pid_rids_cache3 = pid_rids_cache2.map {
        case (rid, pp) => (rid, Util.compute_kendall_tau(query_pp, pp, prefix_len_f2))
      }.cache()

      val threshold2 = pid_rids_cache3.sortBy(_._2).take(cand).last._2

      val rids_f2 = pid_rids_cache3
        .filter { case (rid, dist) => dist <= threshold2 }
        .map { case (rid, dist) => rid }.collect()

      f1_nums.append(rids_f1.length)
      f2_nums.append(rids_f2.length)

      val dist_pred_f1 = this.evaluate_candiates(rids_f1, sc, raw_data, idxcfg, query)
      val dist_pred_f2 = this.evaluate_candiates(rids_f2, sc, raw_data, idxcfg, query)

      val recall_f1 = Evaluate.callRecall(dist_pred_f1, dist_label)
      val recall_f2 = Evaluate.callRecall(dist_pred_f2, dist_label)
      Util.printLog("\t --> %d / %d, recall: %.2f / %.2f, f1_num: %d/%d, f2_num: %d/%d, threshold: %d / %.3f".format
      (idx +
        1, gt.length, recall_f1, recall_f2,
        f1_nums.last, cand, f2_nums.last, cand, threshold, threshold2))

      pred_gt_combine_f1 += ((dist_pred_f1, dist_label))
      pred_gt_combine_f2 += ((dist_pred_f2, dist_label))
    }

    val recall_input_f1 = pred_gt_combine_f1.toArray.map { case (query_pred, ground_truth) => callRecall(query_pred,
      ground_truth)
    }
    val recall_input_f2 = pred_gt_combine_f2.toArray.map { case (query_pred, ground_truth) => callRecall(query_pred,
      ground_truth)
    }
    val mean_recall_f1 = Util.meanDouble(recall_input_f1.toList)
    val mean_recall_f2 = Util.meanDouble(recall_input_f2.toList)
    val mean_f1_num = Util.meanDouble(f1_nums.toList.map(_.toDouble))
    val mean_f2_num = Util.meanDouble(f2_nums.toList.map(_.toDouble))
    Util.printLog(("==> Result, " +
      "prefix_len: %d / %d, " +
      "cand: %d, " +
      "f1_num: %.2f, " +
      "f2_num: %.2f, " +
      "recall %.2f / %.2f").format(
      prefix_len_f1,
      prefix_len_f2,
      cand,
      mean_f1_num,
      mean_f2_num,
      mean_recall_f1,
      mean_recall_f2))
  }

  def evaluate_candiates(rids: Array[Long],
                         sc: SparkContext,
                         raw_data: RDD[(Long, Array[Float])],
                         idxcfg: IdxCfg,
                         query: Array[Float]
                        ): Array[Float] = {
    val hash_set = HashSet(rids: _*)
    val hs_bc = sc.broadcast(hash_set)

    val dist_pred = raw_data.filter { case (rid, ts) =>
      hs_bc.value.contains(rid)
    }.map {
      case (rid, ts) => Util.computeEuDistanceFloat(query, ts)
    }.takeOrdered(idxcfg.knn_K)

    dist_pred
  }
}