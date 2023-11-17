package org.apache.spark.edu.wpi.dsrg.climber.cfg

import org.apache.spark.edu.wpi.dsrg.climber.cfg.IdxCfg.configMap
import org.apache.spark.edu.wpi.dsrg.climber.utils.Util

object DbgCfg extends AbsCfg with Serializable {
  val paas: Array[Int] = configMap("paas").split(",").map(x => x.toInt)
  val cands: Array[Int] = configMap("cands").split(",").map(x => x.toInt)
  val pps: Array[Int] = configMap("pps").split(",").map(x => x.toInt)

  def get_class(): DbgCfg = {
    new DbgCfg(this.paas, this.cands, this.pps)
  }
}

class DbgCfg(
              val paas: Array[Int],
              val cands: Array[Int],
              val pps: Array[Int]
            ) extends Serializable
