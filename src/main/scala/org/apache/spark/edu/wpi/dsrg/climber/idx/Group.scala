package org.apache.spark.edu.wpi.dsrg.climber.idx

import org.apache.spark.edu.wpi.dsrg.climber.cfg.IdxCfg
import org.apache.spark.edu.wpi.dsrg.climber.utils.Util.printLog
import org.apache.spark.edu.wpi.dsrg.climber.utils.{Ops, Util}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext}

import scala.collection.immutable.HashSet

object Group extends Logging with Serializable {
  def cvt_centers_2_pivot_2_gids(sc: SparkContext,
                                 centers: Array[(Int, Array[Short])]): Map[Short, Array[Int]] = {
    sc.parallelize(centers)
      .flatMap { case (c_id, center) => center.map(p_id => (p_id, c_id))
      }.groupByKey()
      .map { case (p_id, iter_c_id) => (p_id, iter_c_id.toArray.sortWith(_ < _)) }
      .collect()
      .toMap
  }

  def retrieve_and_split(sc: SparkContext,
                         p4s_os_weight_sample: RDD[(String, Long)],
                         idxcfg: IdxCfg): (Array[(Int, Array[Short])], Int, Array[(Int, Trie)]) = {
    val centers = this.retrieve(sc, p4s_os_weight_sample, idxcfg)
    val (partition_num, center_2_partition) = this.split_groups(sc, p4s_os_weight_sample, centers, idxcfg)

    (centers, partition_num, center_2_partition)
  }

  def retrieve(sc: SparkContext,
               p4s_os_weight_sample: RDD[(String, Long)],
               idxcfg: IdxCfg): Array[(Int, Array[Short])] = {
    val centers = select_centers(sc, p4s_os_weight_sample, idxcfg)
    this.save_centers(sc, centers, idxcfg)
    centers
  }

  def select_centers(sc: SparkContext,
                     p4s_os_weight_sample: RDD[(String, Long)],
                     idxcfg: IdxCfg): Array[(Int, Array[Short])] = {
    val centers_1st = get_centers_1st(p4s_os_weight_sample, idxcfg.init_group_num)
    val centers_2nd = get_centers_2nd(sc, p4s_os_weight_sample, centers_1st, idxcfg)
    centers_2nd
  }

  def save_centers(sc: SparkContext,
                   centers: Array[(Int, Array[Short])],
                   idxcfg: IdxCfg): Unit = {
    val path = idxcfg.center_path
    Util.removeIfExist(path)

    sc.parallelize(centers).coalesce(1).saveAsObjectFile(path)
    printLog("==> save centers to %s".format(path))
  }

  private def get_centers_1st(p4s_os_weight_sample: RDD[(String, Long)],
                              init_group_num: Int): Array[(Int, Array[Short])] = {
    val center_1st = p4s_os_weight_sample
      .map { case (p4s_os_str, weight) => (Ops.cvt_os_2_oi(p4s_os_str), weight) }
      .reduceByKey((a, b) => a + b)
      .sortBy(_._2, ascending = false)
      .take(init_group_num)
      .zipWithIndex
      .map { case ((p4s_oi_str, weight), c_id) =>
        val center = p4s_oi_str.split(":").map(_.toShort)
        (c_id + 1, center)
      }

    printLog("==> 1st centers num: %d".format(center_1st.length))

    center_1st
  }

  private def get_centers_2nd(sc: SparkContext,
                              p4s_os_weight_sample: RDD[(String, Long)],
                              centers_1st: Array[(Int, Array[Short])],
                              idxcfg: IdxCfg): Array[(Int, Array[Short])] = {
    val pivot_2_gids = cvt_centers_2_pivot_2_gids(sc, centers_1st)
    val p2g_partitioner = new Center_Selection_Partitioner(centers_1st, pivot_2_gids)

    val block_cap_sample = (idxcfg.block_cap * idxcfg.percent).toInt
    printLog("==> block capacity of sampling dataset: %d".format(block_cap_sample))

    val reserve_gids = p4s_os_weight_sample
      .partitionBy(p2g_partitioner)
      .mapPartitions(iter => {
        val pid = TaskContext.getPartitionId()
        val rcd_num = iter.toArray.map(_._2).sum
        Array((pid, rcd_num)).toIterator
      }).filter(x => x._2 > block_cap_sample)
      .map(_._1)
      .collect()

    val reserve_gids_hs = HashSet(reserve_gids: _*)
    val centers_2nd = centers_1st
      .filter { case (gid, center) => reserve_gids_hs.contains(gid) }
      .zipWithIndex
      .map { case ((old_cid, center), new_cid) => (new_cid + 1, center) }

    printLog("==> 2nd centers num: %d".format(centers_2nd.length))
    centers_2nd
  }

  def split_groups(sc: SparkContext,
                   p4s_os_weight: RDD[(String, Long)],
                   centers: Array[(Int, Array[Short])],
                   idxcfg: IdxCfg): (Int, Array[(Int, Trie)]) = {
    val pivot_2_gids = this.cvt_centers_2_pivot_2_gids(sc, centers)
    val p2g_partitioner = new Center_Selection_Partitioner(centers, pivot_2_gids)

    val bin_cap_sample = (idxcfg.block_cap * idxcfg.percent * idxcfg.block_cap_percent).toInt

    Util.removeIfExist(idxcfg.group_2_partition_path)

    val trie_bins_cache = p4s_os_weight
      .partitionBy(p2g_partitioner)
      .mapPartitions(iter => {
        val group_id = TaskContext.getPartitionId()
        val trie = new Trie(iter.toArray, bin_cap_sample)
        val trie_bins = trie.package_node(bin_cap_sample, idxcfg.block_min_percent)
        Iterator((group_id, trie_bins))
      }).zipWithIndex().cache()

    val cur_pid = trie_bins_cache.map { case ((group_id, (trie, bins)), idx) => (idx, bins.length) }.collect()
    val cum_pid_mp = cumulate_pid(cur_pid)
    val cum_pid_mp_bc = sc.broadcast(cum_pid_mp)
    val partition_num = cur_pid.map(_._2).sum

    val centerId_trie_cache = trie_bins_cache.map { case ((group_id, (trie, bins)), idx) =>
      val start_pid = cum_pid_mp_bc.value.getOrElse(idx, 0)
      (group_id, trie.set_pids(bins, start_pid))
    }.cache()

    centerId_trie_cache.saveAsObjectFile(idxcfg.group_2_partition_path)
    Util.printLog("==> partition num: %d".format(partition_num))
    Util.printLog("==> save group_to_partition to %s".format(idxcfg.group_2_partition_path))

    val centerId_trie = centerId_trie_cache.collect()
    (partition_num, centerId_trie)
  }

  private def cumulate_pid(cur_pid: Array[(Long, Int)]): Map[Long, Int] = {
    val output = Array.fill[Int](cur_pid.length)(0)
    for (idx <- 1 until cur_pid.length) {
      output(idx) = output(idx - 1) + cur_pid(idx - 1)._2
    }

    val cum_pid = output.zip(cur_pid)
      .map { case (cum_num, (idx, num)) => (idx, cum_num) }
      .toMap

    cum_pid
  }
}