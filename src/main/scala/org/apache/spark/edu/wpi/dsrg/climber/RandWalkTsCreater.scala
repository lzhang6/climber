package org.apache.spark.edu.wpi.dsrg.climber

import java.io.Serializable

import org.apache.spark.SparkContext
import org.apache.spark.edu.wpi.dsrg.climber.cfg.RwCfg
import org.apache.spark.edu.wpi.dsrg.climber.utils.Util.{convert, normalize, printLog}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd._

object RandWalkTsCreater extends Serializable {
  def apply(sc: SparkContext): Unit = {
    RwCfg.printCfg()
    val rc = new RandWalkTsCreater();
    rc.generateTsAndSave(sc);
  }
}

class RandWalkTsCreater extends Logging with Serializable {
  def generateTsAndSave(sc: SparkContext): Unit = {

    val tsRdd = generateTsRdd(sc,
      RwCfg.rwNumber,
      RwCfg.rwLength,
      RwCfg.rwPartitionNbr,
      RwCfg.rwSeed)

    try {
      val savePath = RwCfg.getSaveHdfsPath()
      tsRdd.saveAsObjectFile(savePath)
      printLog(("==> save data series to %s").format(savePath))
    } catch {
      case e: Exception => logError(e.toString)
        System.exit(0)
    }
  }

  def generateTsRdd(sc: SparkContext,
                    nbr: Long,
                    length: Int,
                    partitionNbr: Int,
                    seed: Long): RDD[(Long, Array[Float])] = {
    RandomRDDs.normalVectorRDD(sc, nbr, length, partitionNbr, seed)
      .map(x => normalize(convert(x.toArray)))
      .zipWithUniqueId()
      .map { case (x, y) => (y, x) }
  }
}