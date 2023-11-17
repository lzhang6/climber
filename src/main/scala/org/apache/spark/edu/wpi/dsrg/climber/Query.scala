package org.apache.spark.edu.wpi.dsrg.climber

import java.io.Serializable
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.edu.wpi.dsrg.climber.cfg.IdxCfg
import org.apache.spark.edu.wpi.dsrg.climber.idx._
import org.apache.spark.edu.wpi.dsrg.climber.utils.Util.printLog
import org.apache.spark.edu.wpi.dsrg.climber.utils.{Evaluate, Ops, Util}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.mutable.ListBuffer

object Query extends Logging with Serializable {
  def apply(sc: SparkContext): Unit = {
    val idxcfg = IdxCfg.getClass(true, false)
    val query = new Query()
    query.execute(sc, idxcfg)
  }
}

class Query extends Logging with Serializable {
  private[climber] def load_ground_truth(sc: SparkContext,
                                         idxcfg: IdxCfg): Array[(Array[Float], Array[Float])] = {
    val ground_truth = sc.objectFile[(Array[Float], Array[Float])](idxcfg.label_path)
      .map { case (query, label) => (query, label.take(idxcfg.knn_K)) }
      .take(idxcfg.query_num)

    Util.printLog("==> read ground truth from disk")
    ground_truth
  }

  def execute(sc: SparkContext, idxcfg: IdxCfg): Unit = {
    val query_with_groundTruth = this.load_ground_truth(sc, idxcfg)
    val pivots_bc = Pivots.read_pivots_bc(sc, idxcfg)

    if (idxcfg.mode == "partition") {
      this.run_partition(sc, idxcfg, query_with_groundTruth, pivots_bc)
    } else {
      this.run_group(sc, idxcfg, query_with_groundTruth, pivots_bc)
    }
  }

  def run_partition(sc: SparkContext,
                    idxcfg: IdxCfg,
                    query_with_groundTruth: Array[(Array[Float], Array[Float])],
                    pivots_bc: Broadcast[Array[(Short, Array[Float])]]
                   ): Unit = {
    val comp_struct = Compress_Structure(sc, idxcfg)

    val pred_gt_combine = ListBuffer.empty[(Array[Float], Array[Float])]
    val size_list = ListBuffer.empty[(Int, Int)]
    val startTime = Calendar.getInstance().getTime().getTime

    for (((query, dist_label), idx) <- query_with_groundTruth.zipWithIndex) {
      try {
        val (dist_pred, candidate_size, partition_size) =
          this.process_one_query_partition_mode(sc, query, pivots_bc.value, comp_struct, idxcfg)

        val recall = Evaluate.callRecall(dist_pred, dist_label)
        val error = Evaluate.calErrorRatio(dist_pred, dist_label)

        printLog("--> [%d/%d] recall: %.3f, error: %.3f, cand_size: %d\n".format(idx + 1,
          idxcfg.query_num, recall, error, candidate_size))
        pred_gt_combine += ((dist_pred, dist_label))
        size_list += ((candidate_size, partition_size))
      } catch {
        case e: java.util.NoSuchElementException => logError(e.toString)
      }
    }

    val duration = Calendar.getInstance().getTime().getTime - startTime
    Evaluate.knn(pred_gt_combine.toList, size_list.toList, duration)
  }

  def run_group(sc: SparkContext,
                idxcfg: IdxCfg,
                query_with_groundTruth: Array[(Array[Float], Array[Float])],
                pivots_bc: Broadcast[Array[(Short, Array[Float])]]
               ): Unit = {

    val csg = Compress_Structure_Group(sc, idxcfg)
    val pred_gt_combine = ListBuffer.empty[(Array[Float], Array[Float])]
    val size_list = ListBuffer.empty[(Int, Int)]

    val startTime = Calendar.getInstance().getTime().getTime

    for (((query, dist_label), idx) <- query_with_groundTruth.zipWithIndex) {
      try {
        val (dist_pred, candidate_size, partition_size) = this.process_one_query_group_mode(sc, query, pivots_bc.value,
          csg, idxcfg)


        val recall = Evaluate.callRecall(dist_pred, dist_label)
        val error = Evaluate.calErrorRatio(dist_pred, dist_label)

        printLog("--> [%d/%d] recall: %.3f, error: %.3f, cand_size: %d\n".format(idx + 1,
          idxcfg.query_num, recall, error, candidate_size))
        pred_gt_combine += ((dist_pred, dist_label))
        size_list += ((candidate_size, partition_size))
      } catch {
        case e: java.util.NoSuchElementException => logError(e.toString)
      }
    }

    val duration = Calendar.getInstance().getTime().getTime - startTime

    Evaluate.knn(pred_gt_combine.toList, size_list.toList, duration)
  }

  def process_one_query_partition_mode(sc: SparkContext,
                                       query: Array[Float],
                                       pivots: Array[(Short, Array[Float])],
                                       comp_struct: Compress_Structure,
                                       idxcfg: IdxCfg
                                      ): (Array[Float], Int, Int) = {
    val paa = Ops.cvtTsToPaa(query, idxcfg.paa_length)
    val p4s_os_str = Ops.cvtPaaTo4PsOs(paa, pivots, idxcfg.pp_length)
    val p4s_os = p4s_os_str.split(":").map(_.toShort)
    val partition_ids = comp_struct.search_partition_mode(p4s_os, idxcfg)

    //    val local_idx_path = partition_ids.map(pid => idxcfg.shuffle_local + "/part-" +
    //      Util.genZero(5 - pid.toString.length) + pid.toString)

    val (output, candidate_size, partition_num) = this.retrieve_data(sc, p4s_os, query, partition_ids, idxcfg)
    (output, candidate_size, partition_num)
  }


  def retrieve_data(sc: SparkContext,
                    query_p4s: Array[Short],
                    query: Array[Float],
                    partition_ids: Array[Int],
                    idxcfg: IdxCfg): (Array[Float], Int, Int) = {
    val LB = ListBuffer.empty[RDD[(Long, Int, Float)]]

    for (pid <- partition_ids) {
      val idx_path = idxcfg.shuffle_local + "/part-" + Util.genZero(5 - pid.toString.length) + pid.toString
      val rdd = sc.objectFile[(Long, Array[Short])](idx_path)
        .map { case (rid, p4s) => (rid, pid, Util.compute_kendall_tau(query_p4s, p4s)) }
      LB += rdd
    }

    val pid_rids_cache = sc.union(LB).cache()

    val cand_num = (idxcfg.cand_scale * idxcfg.knn_K).toInt
    val threshold2 = pid_rids_cache.sortBy(_._3).take(cand_num).last._3

    val rids_cache = pid_rids_cache
      .filter { case (rid, pid, dist) => dist <= threshold2 }
      .cache()

    val rids = rids_cache.map(_._1).collect()
    val bf = BloomFilter.create(rids.length, 0.1)
    for (rid <- rids) {
      bf.putLong(rid)
    }
    //    val hash_set = HashSet(rids: _*)
    //    val hs_bc = sc.broadcast(hash_set)
    val bf_bc = sc.broadcast(bf)

    val pid_paths = rids_cache
      .map { case (rid, pid, dist) => (pid, 1) }
      .reduceByKey((x, y) => x + y)
      .filter { case (pid, num) => num > idxcfg.knn_K * idxcfg.min_scale }
      .map { case (pid, num) => idxcfg.shuffle_path + "/part-" +
        Util.genZero(5 - pid.toString.length) + pid.toString
      }.collect()


    val dist_pred = if (idxcfg.fs) {
      sc.objectFile[(Long, Array[Float])](pid_paths.mkString(","))
        .map {
          case (rid, ts) => Util.computeEuDistanceFloat(query, ts)
        }.takeOrdered(idxcfg.knn_K)
    } else {
      sc.objectFile[(Long, Array[Float])](pid_paths.mkString(","))
        .filter { case (rid, ts) =>
          bf_bc.value.mightContain(rid)
          //          hs_bc.value.contains(rid)
        }.map {
        case (rid, ts) => Util.computeEuDistanceFloat(query, ts)
      }.takeOrdered(idxcfg.knn_K)
    }

    Util.printLog("--> Partition: %d / %d, rid: %d / %.1f, %d".format(partition_ids.length, pid_paths.length,
      rids.length, idxcfg.cand_scale * idxcfg.knn_K, dist_pred.length))
    (dist_pred, rids.length, pid_paths.length)
  }

  def process_one_query_group_mode(sc: SparkContext,
                                   query: Array[Float],
                                   pivots: Array[(Short, Array[Float])],
                                   csg: Compress_Structure_Group,
                                   idxcfg: IdxCfg
                                  ): (Array[Float], Int, Int) = {
    val paa = Ops.cvtTsToPaa(query, idxcfg.paa_length)
    val p4s_os_str = Ops.cvtPaaTo4PsOs(paa, pivots, idxcfg.pp_length)
    val p4s_os = p4s_os_str.split(":").map(_.toShort)
    val group_ids = csg.search_group_mode(p4s_os, idxcfg)
    val path = group_ids.map(gid => idxcfg.dataset_query_path + "/part-" +
      Util.genZero(5 - gid.toString.length) + gid.toString).mkString(",")

    val (output, candidate_size) = this.sub_query_scan(sc, query, path, idxcfg.knn_K)
    (output, candidate_size, group_ids.length)
  }

  def sub_query_scan(sc: SparkContext,
                     query: Array[Float],
                     path: String,
                     k_value: Int): (Array[Float], Int) = {
    val output_cache = sc.objectFile[(String, (Long, Array[Float]))](path)
      .map { case (sign, (id, ts)) =>
        Util.computeEuDistanceFloat(query, ts)
      }
      .cache()

    val candidate_size = output_cache.count().toInt
    val output = output_cache.takeOrdered(k_value)
    (output, candidate_size)
  }
}