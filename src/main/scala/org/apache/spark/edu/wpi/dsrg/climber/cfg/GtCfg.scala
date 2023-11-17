package org.apache.spark.edu.wpi.dsrg.climber.cfg

import org.apache.spark.edu.wpi.dsrg.climber.utils.Util.fetchBoolFromString

object GtCfg extends AbsCfg with Serializable {

  val gt_number: Int = configMap("gt_number").toInt
  var gt_steps: Int = configMap("gt_steps").toInt
  val gt_knnK: Int = configMap("gt_knnK").toInt
  val gt_length: Int = configMap("gt_length").toInt
  val gt_seed: Int = configMap("gt_seed").toInt
  val gt_dataset_path: String = configMap("gt_dataset_path")
  val gt_sample_from_dataset = fetchBoolFromString(configMap("gt_sample_from_dataset"))

  val gt_save_path: String = get_save_path()


  override def toString(): String = {
    ("\n==> Create kNN Ground Truth Configuration" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +
//      "\n * gt_seed\t%d" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%b").format(
      "dsNum",gt_number,
      "step",gt_steps,
      "k-value",gt_knnK,
      "length",gt_length,
//      "seed",gt_seed,
      "dsPath",gt_dataset_path,
      "savePath",gt_save_path,
      "sampData",gt_sample_from_dataset)
  }

  def get_save_path(): String = {
    gt_dataset_path + "-gt" + "/gt-" +
      gt_number.toString + "-" +
      gt_knnK.toString + "-" +
      gt_length.toString + "-" +
      gt_sample_from_dataset.toString
  }

  def getClass(isPrint: Boolean = false): GtCfg = {
    if (isPrint) printCfg();

    new GtCfg(
      this.gt_number,
      this.gt_steps,
      this.gt_knnK,
      this.gt_length,
      this.gt_seed,
      this.gt_dataset_path,
      this.gt_sample_from_dataset,
      this.gt_save_path
    )
  }
}

class GtCfg(
             val gt_number: Int,
             var gt_steps: Int,
             val gt_knnK: Int,
             val gt_length: Int,
             val gt_seed: Int,
             val gt_dataset_path: String,
             val gt_sample_from_dataset: Boolean,
             val gt_save_path: String
           ) extends Serializable
