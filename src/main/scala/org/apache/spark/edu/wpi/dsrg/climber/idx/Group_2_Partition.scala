package org.apache.spark.edu.wpi.dsrg.climber.idx

import org.apache.spark.edu.wpi.dsrg.climber.utils.Util

object Group_2_Partition extends Serializable {
  def apply(group_2_partition: Array[(Int, Trie)]): Group_2_Partition = new Group_2_Partition(group_2_partition)
}

class Group_2_Partition(group_2_partition: Array[(Int, Trie)]) extends Serializable {
  val group_2_partition_mp: Map[Int, Trie] = group_2_partition.toMap

  def get_partition_list(group_id: Int): Array[Int] = {
    group_2_partition_mp.get(group_id) match {
      case Some(t) => t.get_partition_list()
      case None => {
        Util.printLog("ERROR: get partition list for group id: %d".format(group_id))
        Array(0)
      }
    }
  }

  def get_pid(group_id: Int, p4s: String): Int = {
    group_2_partition_mp.get(group_id) match {
      case Some(t) => t.get_pid(p4s)
      case None => {
        Util.printLog("ERROR: get group id: %d for %s".format(group_id, p4s))
        0
      }
    }
  }
}
