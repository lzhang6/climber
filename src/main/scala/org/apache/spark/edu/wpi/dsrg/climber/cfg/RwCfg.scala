package org.apache.spark.edu.wpi.dsrg.climber.cfg

object RwCfg extends AbsCfg {
  val rwSizeG: Long = configMap("rwSizeG").toLong
  val rwLength: Int = configMap("rwLength").toInt

  val rwSeed: Long = configMap("rwSeed").toLong
  val rwDirPath: String = configMap("rwDirPath")

  val rwNumber = this.cal_record_numer(rwSizeG, rwLength)
  val rwPartitionNbr: Int = calPartitionNbr()
  val rcd_per_part = (rwNumber / rwPartitionNbr).toInt

  def getSaveHdfsPath(): String = {
    rwDirPath + "/TS-" + rwSizeG.toString + "-" + rwLength.toString + "-" + rwPartitionNbr.toString
  }

  def cal_record_numer(data_size: Long, dimen_len: Int): Long = {
    (268008341L * data_size / dimen_len).toLong
  }

  override def toString: String = {
    ("\n==> Create RandomWalk Dataset Configuration" +
      "\n * %-10s\t%d GB" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d MB" +
      "\n * %-10s\t%s").format(
      "dsSize", rwSizeG,
      "dsNum", rwNumber,
      "dsLen", rwLength,
      "partNum", rwPartitionNbr,
      "rcd/Part", rcd_per_part,
      "blockSize", blockSize,
      "savePath", getSaveHdfsPath())
  }

  private def calPartitionNbr(rate: Double = 1.05): Int = {
    val totalSize = this.rwNumber * (this.rwLength * 4 + 8) * rate / (1024 * 1024)
    math.ceil(totalSize / this.blockSize).toInt
  }
}