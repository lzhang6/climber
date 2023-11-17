package org.apache.spark.edu.wpi.dsrg.climber.cfg

import org.apache.spark.edu.wpi.dsrg.climber.utils.Util
import org.apache.spark.edu.wpi.dsrg.climber.utils.Util.printLog
import org.apache.spark.internal.Logging

import java.io.Serializable

class AbsCfg extends Logging with Serializable {
  val configMap: Map[String, String] = Util.readConfigFile("./etc/config.conf")
  val logPath = configMap("logFileName")
  val blockSize = configMap("blockSize").toInt

  def printCfg(): Unit = printLog(this.toString, true)
}

