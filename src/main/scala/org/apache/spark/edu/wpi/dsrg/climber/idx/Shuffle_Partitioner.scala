package org.apache.spark.edu.wpi.dsrg.climber.idx

import org.apache.spark.Partitioner

class Shuffle_Partitioner(centers: Array[(Int, Array[Short])],
                          pivot_2_gids: Map[Short, Array[Int]],
                          partition_num: Int,
                          center_2_partition: Array[(Int, Trie)]
                         ) extends Partitioner {
  val pivot_2_group: Pivot_2_Group = Pivot_2_Group(centers, pivot_2_gids)
  val group_2_partition: Group_2_Partition = Group_2_Partition(center_2_partition)

  override def numPartitions: Int = partition_num

  override def getPartition(key: Any): Int = {
    val p4s_os_str = key.asInstanceOf[String]
    val group_id = this.pivot_2_group.get_gid(p4s_os_str)
    val pid = this.group_2_partition.get_pid(group_id, p4s_os_str)
    pid
  }

  override def equals(other: scala.Any): Boolean = other match {
    case temp: Shuffle_Partitioner => temp.numPartitions == this.numPartitions
    case _ => false
  }
}
