package org.apache.spark.edu.wpi.dsrg.climber.utils

import org.apache.spark.internal.Logging

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting.quickSort

object Ops extends Logging with Serializable {
  def cvtTsToPaa(ts: Array[Float],
                 segment_length: Int): Array[Float] = {
    ts.sliding(segment_length, segment_length)
      .map(x => x.sum / x.length)
      .toArray
  }

  def cvtPaaTo4Ps_os(paa: Array[Float],
                     pivots: Array[(Short, Array[Float])],
                     prefix_length: Int): Array[Short] = {
    var prefix_map = scala.collection.mutable.Map[Float, Short]()
    prefix_map += (Float.MaxValue -> (-1).toShort)

    var max_dist = prefix_map.keys.max

    for ((pivot_id, pivot_value) <- pivots) {
      val dist = Util.computeEuDistanceFloat(paa, pivot_value)
      if (dist < max_dist) {
        if (prefix_map.size < prefix_length) {
          prefix_map += (dist -> pivot_id)
        } else {
          prefix_map -= (max_dist)
          prefix_map += (dist -> pivot_id)
          max_dist = prefix_map.keys.max
        }
      }
    }

    prefix_map.toArray
      .sortWith(_._1 < _._1)
      .map(_._2)
  }

  def cvtPaaTo4Ps_os_whole(paa: Array[Float],
                           pivots: Array[(Short, Array[Float])]): Array[Short] = {
    val dist_pivots_bf = ArrayBuffer.empty[(Float, Short)]

    for ((pivot_id, pivot_value) <- pivots) {
      val dist = Util.computeEuDistanceFloat(paa, pivot_value)
      dist_pivots_bf.append((dist, pivot_id))
    }

    val dist_pivots = dist_pivots_bf.toArray
    quickSort(dist_pivots)(Ordering[(Float, Short)].on(x => (x._1, x._2)))
    dist_pivots.map(_._2)
  }

  def cvtPaaTo4Ps_os_str(paa: Array[Float],
                         pivots: Array[(Short, Array[Float])],
                         prefix_length: Int): String = {
    val p4s = this.cvtPaaTo4Ps_os(paa, pivots, prefix_length)
    p4s.mkString(":")
  }

  def cvtPaaToPermutation(paa: Array[Float],
                          pivots: Array[(Short, Array[Float])]): Array[(Short, Int)] = {
    val dists = pivots.map { case (pid, pivot) => (Util.computeEuDistanceFloat(paa, pivot), pid) }
    quickSort(dists)
    dists.zipWithIndex.map { case ((dist, pid), pos) => (pid, pos) }
  }

  //
  //  def cvtPaaTo4PsOs_and_allPivots(paa: Array[Float],
  //                                  pivots: Array[(Short, Array[Float])],
  //                                  prefix_length: Int): (String, Array[(Short, Int)]) = {
  //    val dists = pivots.map { case (pid, p_paa) => (Util.computeEuDistanceFloat(paa, p_paa), pid) }
  //    quickSort(dists)
  //
  //    val p4s_os = dists.take(prefix_length).map(_._2)
  //    val pivot_perm_all = dists.zipWithIndex.map { case ((dist, pid), idx) => (pid, idx + 1) }
  //    (p4s_os.mkString(":"), pivot_perm_all)
  //  }

  def cvtPaaTo4PsOs(paa: Array[Float],
                    pivots: Array[(Short, Array[Float])],
                    prefix_length: Int): String = {
    val dists = pivots.map { case (pid, p_paa) => (Util.computeEuDistanceFloat(paa, p_paa), pid) }
    quickSort(dists)

    val p4s_os = dists.take(prefix_length).map(_._2)
    p4s_os.mkString(":")
  }

  def cvtTsTo4Ps_os(ts: Array[Float],
                    segment_length: Int,
                    pivots: Array[(Short, Array[Float])],
                    prefix_length: Int
                   ): Array[Short] = {
    val paa = cvtTsToPaa(ts, segment_length)
    val perm = cvtPaaTo4Ps_os(paa, pivots, prefix_length)
    perm
  }

  def cvt_os_2_oi(p4s_os_str: String): String = {
    p4s_os_str.split(":")
      .map(p => p.toShort)
      .sortWith(_ < _)
      .mkString(":")
  }

  def sort_dists(dist_id_s: Seq[Float], k_value: Int): Array[Float] = {
    val quickArray = dist_id_s.slice(0, k_value).toArray
    quickSort(quickArray)

    if (dist_id_s.size > k_value) {
      for (elem <- dist_id_s.slice(k_value, dist_id_s.size)) {
        if (elem < quickArray.last) {
          quickArray(k_value - 1) = elem
          quickSort(quickArray)
        }
      }
    }
    quickArray
  }
}