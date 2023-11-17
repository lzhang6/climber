package org.apache.spark.edu.wpi.dsrg.climber

import java.io.Serializable

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.edu.wpi.dsrg.climber.cfg.IdxCfg
import org.apache.spark.edu.wpi.dsrg.climber.idx._
import org.apache.spark.edu.wpi.dsrg.climber.utils.{Ops, Statistic, Util}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

object Index extends Serializable {
  def apply(sc: SparkContext): Unit = {
    val idxcfg = IdxCfg.getClass(true, true)
    val idx = new Index
    idx.construct(sc, idxcfg)
  }
}

class Index extends Logging with Serializable {
  def construct(sc: SparkContext, idxcfg: IdxCfg): Unit = {
    val (paa_rdd_cache, pivots_bc) = this.sample_pivots(sc, idxcfg)
    val p4s_os_weight_sample = paa_rdd_cache
      .map { paa => {
        val p4s_os = Ops.cvtPaaTo4Ps_os(paa, pivots_bc.value, idxcfg.pp_length)
        val p4s_os_str = p4s_os.mkString(":")
        (p4s_os_str, 1L)
      }
      }.reduceByKey((a, b) => a + b)
      .cache()

    if (idxcfg.mode == "partition") {
      val (centers, partition_num, center_2_partition) = Group.retrieve_and_split(sc, p4s_os_weight_sample, idxcfg)
      this.shuffle_data_partition_mode(sc, centers, partition_num, center_2_partition, idxcfg)
    } else {
      val centers = Group.retrieve(sc, p4s_os_weight_sample, idxcfg)
      this.shuffle_data_group_mode(sc, centers, idxcfg)
    }

    this.index_quality(sc, idxcfg)
  }

  def sample_pivots(sc: SparkContext, idxcfg: IdxCfg): (RDD[Array[Float]], Broadcast[Array[(Short, Array[Float])]]) = {
    val sample_paths = Util.get_sample_block_paths(idxcfg)

    if (! Util.hdfsDirExists(idxcfg.pivot_path_raw)) {
      val pivots_raw = sc.objectFile[(Long, Array[Float])](sample_paths)
        .takeSample(withReplacement = false, idxcfg.pivot_num)
        .zipWithIndex
        .map { case ((rid, pivot), id) => ((id + 1).toShort, pivot) }

      sc.parallelize(pivots_raw).saveAsObjectFile(idxcfg.pivot_path_raw)
      Util.printLog("==> save pivots (data series) to %s".format(idxcfg.pivot_path_raw))
    }
    val paa_rdd_cache = sc.objectFile[(Long, Array[Float])](sample_paths)
      .map { case (id, ts) => Ops.cvtTsToPaa(ts, idxcfg.paa_length) }
      .cache()

    val pivots_bc = Pivots.get_pivot_bc(sc, idxcfg)
    (paa_rdd_cache, pivots_bc)
  }

  def shuffle_data_partition_mode(sc: SparkContext,
                                  centers: Array[(Int, Array[Short])],
                                  partition_num: Int,
                                  center_2_partition: Array[(Int, Trie)],
                                  idxcfg: IdxCfg): Unit = {
    Util.printLog("==> start shuffle whole dataset[PARTITION MODE]")
    val pivot_2_gids = Group.cvt_centers_2_pivot_2_gids(sc, centers)
    val shuffle_partitioner = new Shuffle_Partitioner(centers, pivot_2_gids, partition_num, center_2_partition)
    val pivots_bc = Pivots.read_pivots_bc(sc, idxcfg)

    Util.removeIfExist(idxcfg.shuffle_path)

    val rs_shuffle = sc.objectFile[(Long, Array[Float])](idxcfg.dataset_path).map { case (id, ts) =>
      val paa = Ops.cvtTsToPaa(ts, idxcfg.paa_length)
      val p4s_os_str = Ops.cvtPaaTo4Ps_os_str(paa, pivots_bc.value, idxcfg.pp_length)
      (p4s_os_str, (id, ts))
    }.partitionBy(shuffle_partitioner).cache()

    rs_shuffle.map { case (p4s_os_str, (id, ts)) => (id, p4s_os_str.split(":").map(_.toShort)) }
      .saveAsObjectFile(idxcfg.shuffle_local)
    Util.printLog("==> save shuffle local_index to %s".format(idxcfg.shuffle_local))

    rs_shuffle.map { case (p4s_os_str, (id, ts)) => (id, ts) }
      .saveAsObjectFile(idxcfg.shuffle_path)
    Util.printLog("==> save shuffle data to %s".format(idxcfg.shuffle_path))
  }

  def shuffle_data_group_mode(sc: SparkContext,
                              centers: Array[(Int, Array[Short])],
                              idxcfg: IdxCfg): Unit = {
    Util.printLog("==> start shuffle whole dataset[GROUP MODE]")
    val pivot_2_gids = Group.cvt_centers_2_pivot_2_gids(sc, centers)
    val shuffle_grouper = new Shuffle_Grouper(centers, pivot_2_gids)
    val pivots_bc = Pivots.read_pivots_bc(sc, idxcfg)

    Util.removeIfExist(idxcfg.shuffle_path)

    sc.objectFile[(Long, Array[Float])](idxcfg.dataset_path).map { case (id, ts) =>
      val paa = Ops.cvtTsToPaa(ts, idxcfg.paa_length)
      val p4s_os_str = Ops.cvtPaaTo4Ps_os_str(paa, pivots_bc.value, idxcfg.pp_length)
      (p4s_os_str, (id, ts))
    }.partitionBy(shuffle_grouper)
      .saveAsObjectFile(idxcfg.shuffle_path)

    Util.printLog("==> save shuffle data to %s".format(idxcfg.shuffle_path))
  }

  def index_quality(sc: SparkContext, idxcfg: IdxCfg): Unit = {
    var content = "==> Index Information" +
      "\n++> Raw Data Series---------------" + Statistic(idxcfg.dataset_path).fetchAll() +
      "\n++> Shuffled Data Series----------" + Statistic(idxcfg.shuffle_path).fetchAll() +
      "\n++> Histogram of Shuffled Data----" + Statistic(idxcfg.shuffle_path).histogram(sc, idxcfg) +
      "\n++> Index: Center-----------------" + Statistic(idxcfg.center_path).fetchSizeM()

    if (idxcfg.mode != "group") {
      content = content +
        "\n++> Index: Group_2_partition------" + Statistic(idxcfg.group_2_partition_path).fetchSizeM()
    }

    Util.printLog(content)
  }
}