package org.apache.spark.edu.wpi.dsrg.climber.idx

import org.apache.spark.Partitioner

class Center_Selection_Partitioner(centers: Array[(Int, Array[Short])],
                                   pivot_2_gids: Map[Short, Array[Int]]
                                  ) extends Partitioner {
  val p2g: Pivot_2_Group = Pivot_2_Group(centers, pivot_2_gids)

  override def numPartitions: Int = centers.length + 1

  override def getPartition(key: Any): Int = {
    val p4s_os_str = key.asInstanceOf[String]
    val pid = this.p2g.get_gid(p4s_os_str)
    pid
  }

  override def equals(other: scala.Any): Boolean = other match {
    case temp: Center_Selection_Partitioner => temp.numPartitions == this.numPartitions
    case _ => false
  }
}