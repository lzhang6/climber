package org.apache.spark.edu.wpi.dsrg.climber.idx

import org.apache.spark.Partitioner

class Shuffle_Grouper(centers: Array[(Int, Array[Short])],
                      pivot_2_gids: Map[Short, Array[Int]]
                     ) extends Partitioner {
  val pivot_2_group: Pivot_2_Group = Pivot_2_Group(centers, pivot_2_gids)

  override def numPartitions: Int = centers.length + 1

  override def getPartition(key: Any): Int = {
    val p4s_os_str = key.asInstanceOf[String]
    val group_id = this.pivot_2_group.get_gid(p4s_os_str)
    group_id
  }

  override def equals(other: scala.Any): Boolean = other match {
    case temp: Shuffle_Grouper => temp.numPartitions == this.numPartitions
    case _ => false
  }
}