package org.apache.spark.edu.wpi.dsrg.climber.idx

import org.apache.spark.SparkContext
import org.apache.spark.edu.wpi.dsrg.climber.cfg.IdxCfg
import org.apache.spark.edu.wpi.dsrg.climber.utils.Util

import scala.collection.mutable

object Compress_Structure_Group extends Serializable {
  def apply(sc: SparkContext,
            idxcfg: IdxCfg): Compress_Structure_Group = {
    val csg = new Compress_Structure_Group()

    csg.read(sc, idxcfg)
    csg
  }
}


class Compress_Structure_Group extends Serializable {
  //group_id -> center_p4s_pi
  private var centers_mp: Map[Int, Array[Short]] = Map.empty[Int, Array[Short]]
  //pivot_id -> group_ids
  private var pivot_2_gids_mp: Map[Short, Array[Int]] = Map.empty[Short, Array[Int]]
  // group_id -> Array[(partition_id, Map[pivot_id -> weight])]
  // weight = rcd_nun/block_cap

  def read(sc: SparkContext,
           idxcfg: IdxCfg): Unit = {
    val centers = sc.objectFile[(Int, Array[Short])](idxcfg.center_path).collect()
    //    val group_2_trie = sc.objectFile[(Int, Trie)](idxcfg.group_2_partition_path).collect()
    this.construct(sc, centers)
  }

  def construct(sc: SparkContext,
                centers: Array[(Int, Array[Short])]): Unit = {
    this.centers_mp = centers.toMap
    this.pivot_2_gids_mp = Group.cvt_centers_2_pivot_2_gids(sc, centers)
    //    this.group_2_bins_mp = group_2_trie.map { case (group_id, trie) => (group_id, trie.pid_weight) }.toMap
  }

  def search_group_mode(p4s_os: Array[Short], idxcfg: IdxCfg): Array[Int] = {
    val gids_dist = this.get_group_ids(p4s_os)

    if (gids_dist.length != 0) {

      val gids_weight = this.evaluate_group_weight(gids_dist, p4s_os)
      val gids = this.obtain_fix_area(gids_weight, idxcfg.scope)
      Util.printLog("--> Grp_num: %d / %d".format(gids.length,
        gids_weight.length))
      gids
    } else {
      Util.printLog("--> Grp_num: 1 / 1")
      Array(0)
    }
  }

  private[climber] def obtain_fix_area(pids_weight: Array[(Int, Float)],
                                       scope: Float): Array[Int] = {
    val pids_weight_sort = pids_weight.filter(_._2 > 0).sortWith(_._2 < _._2)

    val accumu_weight = pids_weight_sort.clone()
    for (idx <- 1 until (accumu_weight.length)) {
      val value = (accumu_weight(idx)._1, accumu_weight(idx)._2 + accumu_weight(idx - 1)._2)
      accumu_weight.update(idx, value)
    }

    val total_area = accumu_weight.last._2
    val accumu_weight_area = accumu_weight.map { case (idx, value) => (idx, value / total_area) }

    val pids = accumu_weight_area.filter(_._2 > 1 - scope).map(_._1)
    pids
  }

  private[climber] def get_group_ids(p4s_os: Array[Short]): Array[(Int, Int)] = {
    // return: (group_id, distance)

    val gids = p4s_os.flatMap(pivot_id =>
      this.pivot_2_gids_mp.get(pivot_id) match {
        case Some(gids) => gids;
        case None => Array.empty[Int];
      }
    )

    val gid_dist_hm = mutable.HashMap.empty[Int, Int]

    for (gid <- gids) {
      if (gid_dist_hm.contains(gid)) {
        gid_dist_hm(gid) += 1
      } else {
        gid_dist_hm += (gid -> 1)
      }
    }

    gid_dist_hm.toArray
  }

  private[climber] def evaluate_group_weight(gids_dist: Array[(Int, Int)],
                                             p4s_os: Array[Short]): Array[(Int, Float)] = {
    val gids_weight = gids_dist.map { case (gid, dist) =>
      val center = this.centers_mp.getOrElse(gid, Array.empty[Short])
      val group_weight = if (center.isEmpty) {
        0.0F
      } else {
        this.cvt_center_to_weight(p4s_os, center)
      }
      (gid, group_weight)
    }.sortWith(_._2 < _._2)

    gids_weight
  }


  private[climber] def cvt_center_to_weight(p4s_os: Array[Short],
                                            center: Array[Short]): Float = {
    val pivot_weight_mp = p4s_os.zipWithIndex.map { case (pivot, idx) => (pivot, 1.0 / (idx + 1)) }.toMap
    val total_weight = center.map(pivot => pivot_weight_mp.getOrElse(pivot, 0.0)).sum
    total_weight.toFloat
  }
}