package org.apache.spark.edu.wpi.dsrg.climber.idx

import org.apache.spark.edu.wpi.dsrg.climber.utils.Util

import scala.collection.mutable
import scala.util.Random


object Pivot_2_Group extends Serializable {
  def apply(centers: Array[(Int, Array[Short])],
            pivot_2_gids: Map[Short, Array[Int]]): Pivot_2_Group = new Pivot_2_Group(centers, pivot_2_gids)
}

class Pivot_2_Group(val centers: Array[(Int, Array[Short])],
                    val pivot_2_gids: Map[Short, Array[Int]])
  extends Serializable {
  val center_str_map: Map[String, Int] = centers.map { case (id, p4s_oi) => (p4s_oi.mkString(":"), id) }.toMap
  val center_map: Map[Int, Array[Short]] = centers.toMap

  def get_gid(p4s_os_str: String): Int = {
    val p4s_os = p4s_os_str.split(":").map(_.toShort)
    val p4s_oi_str = p4s_os.sortWith(_ < _).mkString(":")

    center_str_map.get(p4s_oi_str) match {
      case Some(gid) => gid;
      case None => distribute_strategy(p4s_os)
    }
  }

  private[climber] def distribute_strategy(p4s_os: Array[Short]): Int = {
    val gids_dist = get_touch_gids_with_dist(p4s_os)
    val gid_max = gids_with_max_freq(gids_dist)
    val gid = select_gid(gid_max, p4s_os)
    gid
  }

  private[climber] def get_touch_gids_with_dist(p4s_os: Array[Short]): Array[(Int, Int)] = {
    val gids = p4s_os.flatMap(pivot_id =>
      pivot_2_gids.get(pivot_id) match {
        case Some(gids) => gids;
        case None => Array.empty[Int];
      }
    )

    val hm = mutable.HashMap.empty[Int, Int]

    for (gid <- gids) {
      if (hm.contains(gid)) {
        hm(gid) += 1
      } else {
        hm += (gid -> 1)
      }
    }

    val gids_ary = hm.toArray
    //    Util.printLog("--> "+ gids_ary.sortWith(_._2 > _._2).map(x=>"[%d,%d] ".format(x._1, x._2)).mkString(","))
    gids_ary
  }

  private[climber] def gids_with_max_freq(gids: Array[(Int, Int)]): Array[Int] = {
    if (gids.length < 1) {
      Array(0);
    } else {
      val max_freq = gids.map(_._2).max
      val gids_max = gids.filter(_._2 == max_freq).map(_._1)
      gids_max
    }
  }

  //  def search_with_fixed_num(p4s_os_str: String, scope: Int): Array[Int] = {
  //    val p4s_os = p4s_os_str.split(":").map(_.toShort)
  //    val gids_dist = get_touch_gids_with_dist(p4s_os)
  //    val gid_max = this.gids_with_max_freq(gids_dist)(0)
  //
  //    val init_gids = if (gids_dist.length < 1) {
  //      Array(0);
  //    } else {
  //      gids_dist
  //        .sortWith(_._2 > _._2)
  //        .take(scope)
  //        .map(_._1)
  //    }
  //
  //    val output = if (init_gids.contains(gid_max)) {
  //      init_gids
  //    } else {
  //      init_gids(init_gids.length - 1) = gid_max
  //      init_gids
  //    }
  //    output
  //  }

  //  def search_with_weight(p4s_os_str: String, scope: Float): Array[Int] = {
  //    val p4s_os = p4s_os_str.split(":").map(_.toShort)
  //    val gids_dist = get_touch_gids_with_dist(p4s_os)
  //
  //    //    println("==> %s".format(gids_dist.mkString(",")))
  //
  //    val gids_weight = gids_dist.map { case (gid, dist) =>
  //      val center = this.center_map.getOrElse(gid, Array.empty[Short])
  //      val weight = if (center.isEmpty) {
  //        0.0F
  //      } else {
  //        this.cvt_center_to_weight(p4s_os, center)
  //      }
  //      (gid, weight)
  //    }.sortWith(_._2 < _._2)
  //
  //
  //    val accumu_weight = gids_weight.clone()
  //    for (idx <- 1 until (accumu_weight.length)) {
  //      val value = (accumu_weight(idx)._1, accumu_weight(idx)._2 + accumu_weight(idx - 1)._2)
  //      accumu_weight.update(idx, value)
  //    }
  //
  //    val total_area = accumu_weight.last._2
  //    val accumu_weight_area = accumu_weight.map { case (idx, value) => (idx, value / total_area) }
  //
  //    val gids = accumu_weight_area.filter(_._2 > scope).map(_._1)
  //    Util.printLog("\t\t* %s".format(gids_dist.sortWith(_._2 < _._2).mkString(",")))
  //    Util.printLog("\t\t* %s".format(gids_weight.mkString(",")))
  //    Util.printLog("\t\t* %s".format(accumu_weight.mkString(",")))
  //    Util.printLog("\t\t* %s".format(accumu_weight_area.mkString(",")))
  //    Util.printLog("\t\t* %s".format(gids.mkString(",")))
  //    gids
  //  }

  //  private[climber] def cvt_center_to_weight(p4s_os: Array[Short], center: Array[Short]): Float = {
  //    val pivot_weight_mp = p4s_os.zipWithIndex.map { case (pivot, idx) => (pivot, 1.0 / (idx + 1)) }.toMap
  //    val total_weight = center.map(pivot => pivot_weight_mp.getOrElse(pivot, 0.0)).sum
  //    total_weight.toFloat
  //  }

  private[climber] def select_gid(gids: Array[Int],
                                  p4s_os: Array[Short]): Int = {
    val gid = if (gids.length == 1) {
      gids(0);
    } else {
      var prior = gids(0)
      for (idx <- 1 until gids.length) {
        val cur = gids(idx)
        val prior_sign = center_map(prior)
        val cur_sign = center_map(cur)
        prior = if (compare_signs(prior_sign, cur_sign, p4s_os)) {
          prior
        } else {
          cur
        }
      }
      prior
    }

    gid
  }

  private[climber] def compare_signs(left: Array[Short],
                                     right: Array[Short],
                                     p4s_order: Array[Short]): Boolean = {
    val left_p4s = p4s_order.filter(x => left.contains(x)).toSet
    val right_p4s = p4s_order.filter(x => right.contains(x)).toSet

    val comm_part = left_p4s.intersect(right_p4s)
    val unique_part = left_p4s.union(right_p4s).diff(comm_part)

    if (unique_part.nonEmpty) {
      val p4s_temp = p4s_order.filter(x => unique_part.contains(x))
      left.contains(p4s_temp(0))
    } else {
      Random.nextBoolean()
    }
  }
}