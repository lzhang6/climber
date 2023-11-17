package org.apache.spark.edu.wpi.dsrg.climber.idx

import org.apache.spark.internal.Logging

import scala.collection.immutable.HashSet
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class Trie extends Serializable with Logging {
  private val root: TrieNode = TrieNode("root", 0)
  private var default_pid = -1
  private var start_partition_id = -1
  private var partition_num = 0
  //  private var pid_pivots = Array.empty[(Int, Array[Short])]
  var pid_weight = Array.empty[(Int, Map[Short, Float])]

  def this(p4s_os_weight: Array[(String, Long)],
           block_cap: Int) {
    this();
    val rcd_num = p4s_os_weight.map(_._2).sum.toInt;
    this.root.set_rcd_num(rcd_num);
    this.construct(p4s_os_weight, block_cap)
  }

  def construct(p4s_os_weight: Array[(String, Long)],
                block_cap: Int): Unit = {
    var pro_next_level = true
    var level = 1
    var start_data = p4s_os_weight.map { case (p4s, weight) => (p4s, weight.toInt) }

    do {
      val process_data = start_data
        .map { case (p4s, weight) => (p4s.split(":").take(level).mkString(":"), p4s, weight.toInt) }

      val data_nodes = process_data
        .map { case (pivot_str, p4s, weight) => (pivot_str, weight) }
        .groupBy(_._1)
        .map { case (pivots, values) => (pivots, values.map(_._2).sum) }
        .toArray
      this.insert_nodes(data_nodes)

      val fat_nodes = data_nodes.filter(_._2 > block_cap)
      pro_next_level = fat_nodes.length > 0

      if (pro_next_level) {
        val hs = HashSet(fat_nodes.map(_._1): _*)
        start_data = process_data
          .filter { case (pivot_str, p4s, weight) => hs.contains(pivot_str) }
          .map { case (pivot_str, p4s, weight) => (p4s, weight) }
        level += 1
      }
    } while (pro_next_level)
  }

  def insert_nodes(nodes_input: Array[(String, Int)]): Unit = {
    for (node_input <- nodes_input) {
      val pivots = node_input._1
      val rcd_num = node_input._2
      val node = TrieNode(pivots, rcd_num)
      root.insert_node(node)
    }
  }

  def package_node(bin_cap: Int,
                   min_percent: Float): (Trie, Array[Array[(String, Int)]]) = {
    val all_nodes = collect_nodes()
    val bins = fit_all_nodes(all_nodes, bin_cap, min_percent)
    (this, bins)
  }

  private[climber] def collect_nodes(): Array[Array[Array[(String, Int)]]] = {
    val all_nodes = ArrayBuffer.empty[Array[Array[(String, Int)]]]
    var start_nodes: Array[TrieNode] = Array(this.root)

    do {
      val node_level_all = start_nodes.map(node => node.children.values.toArray)
      val node_small = node_level_all.map(node_ary =>
        node_ary.filter(node => node.children.isEmpty).map(node => (node.pivots, node.rcd_num))
      )
      all_nodes += node_small
      start_nodes = node_level_all.flatMap(node_ary => node_ary.filter(node => node.children.nonEmpty))
    } while (!start_nodes.isEmpty)

    all_nodes.toArray
  }

  private[climber] def fit_all_nodes(all_nodes: Array[Array[Array[(String, Int)]]],
                                  bin_cap: Int,
                                  min_percent: Float): Array[Array[(String, Int)]] = {
    var rem_prior_layer: Array[(String, Int)] = Array.empty[(String, Int)]
    val bins_bf = ArrayBuffer.empty[Array[(String, Int)]]

    for (layer_id <- Range(all_nodes.length, 0, -1)) {
      val nodes_layers = all_nodes(layer_id - 1)
      val nodes_layers_rem = ArrayBuffer.empty[(String, Int)]
      for (nodes <- nodes_layers) {
        val (bins, nodes_rem) = this.first_fit_decrease(nodes, bin_cap, min_percent)
        bins_bf ++= bins
        if (!nodes_rem.isEmpty) {
          nodes_layers_rem ++= nodes_rem
        }
      }

      nodes_layers_rem ++= rem_prior_layer

      if (nodes_layers_rem.nonEmpty) {
        val rem_min_percent = if (layer_id == 1) {
          0F
        } else {
          min_percent
        }
        val rem_bin_output = this.first_fit_decrease(nodes_layers_rem.toArray, bin_cap, rem_min_percent)
        bins_bf ++= rem_bin_output._1
        rem_prior_layer = rem_bin_output._2
      }
    }

    bins_bf.toArray
  }

  private[climber] def first_fit_decrease(nodes: Array[(String, Int)],
                                       bin_cap: Int,
                                       min_percent: Float): (Array[Array[(String, Int)]], Array[(String, Int)]) = {
    val bins: ListBuffer[ListBuffer[(String, Int)]] = ListBuffer(new ListBuffer[(String, Int)])
    val bins_rem: ArrayBuffer[Int] = ArrayBuffer(bin_cap)

    val nodes_sort = nodes.sortWith(_._2 > _._2)

    for (node <- nodes_sort) {
      var found_it = false
      for ((rem, bin_id) <- bins_rem.zipWithIndex if !found_it) {
        if (rem >= node._2) {
          bins_rem(bin_id) = rem - node._2
          bins(bin_id) += node
          found_it = true
        }
      }

      if (!found_it) {
        bins_rem += bin_cap - node._2
        bins += new ListBuffer[(String, Int)]
        bins.last += node
      }
    }

    val all_bins = bins.toArray.map(bin => bin.toArray)

    val output_bins = all_bins.filter(bin => bin.map(_._2).sum >= (bin_cap * min_percent).toInt)
    val nodes_small = all_bins.filterNot(bin => bin.map(_._2).sum >= (bin_cap * min_percent).toInt).flatten

    (output_bins, nodes_small)
  }

  def set_pids(bins: Array[Array[(String, Int)]], start_pid: Int): Trie = {
    val bins_pid = bins.zipWithIndex.map { case (bin, pid) => (pid + start_pid, bin) }
    for ((pid, bin) <- bins_pid) {
      for (node <- bin) {
        this.root.update_pid(node, pid)
      }
    }

    this.default_pid = try {
      bins_pid
        .map { case (pid, bin) => (pid, bin.map(_._2).sum) }
        .minBy(_._2)
        ._1
    } catch {
      case ex: Exception => {
        val content = ex.toString + "\n" +
          bins_pid.map { case (pid, bin) => (pid, bins.map { case (x) => (x.mkString(" | ")) }.mkString("; ")) }
            .mkString(": ") + "\n" +
          "SET THE DEFAULT PID"

        logError(content)

        start_pid
      }
    }
    //    this.default_pid = bins_pid
    //      .map { case (pid, bin) => (pid, bin.map(_._2).sum) }
    //      .minBy(_._2)
    //      ._1

    this.start_partition_id = start_pid
    this.partition_num = bins.length

    //    this.pid_pivots = bins_pid.map { case (pid, bin) => {
    //      val pivots = bin
    //        .flatMap { case (node, weight) => node.split(":").map(_.toShort) }
    //        .distinct
    //        .sortWith(_<_)
    //      (pid, pivots)
    //    }
    //    }

    this.pid_weight = bins_pid.map { case (pid, bin) =>
      val total_weight = bin.map(_._2).sum
      val pivot_weight_mp = bin.map { case (node, num) => (node.split(":").head.toShort, num) }
        .groupBy(_._1)
        .map(x => (x._1, (x._2.map(_._2).sum) * 1.0F / total_weight))
      (pid, pivot_weight_mp)
    }

    this
  }

  def get_partition_list(): Array[Int] = {
    val partition_list = this.start_partition_id until (this.start_partition_id + this.partition_num)
    partition_list.toArray
  }

  //  def get_pivots_mp(): Map[Int, Array[Short]] = {
  //    this.pid_pivots.toMap
  //  }

  def get_pid(p4s: String): Int = {
    val pid = this.root.assign_pid(p4s)
    if (pid == -1) {
      this.default_pid
    } else {
      pid
    }
  }

  def print_values(): Unit = {
    println(this.root.toString())
  }
}