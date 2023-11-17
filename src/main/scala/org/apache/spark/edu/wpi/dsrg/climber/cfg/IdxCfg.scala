package org.apache.spark.edu.wpi.dsrg.climber.cfg

import org.apache.spark.edu.wpi.dsrg.climber.utils.Util
import org.apache.spark.edu.wpi.dsrg.climber.utils.Util.fetchBoolFromString

object IdxCfg extends AbsCfg with Serializable {
  //mode: group or partition
  val mode: String = this.get_mode()

  //  4Ps config
  val ts_length: Int = configMap("ts_length").toInt
  val paa_length: Int = configMap("paa_length").toInt
  val percent: Float = configMap("percent").toFloat
  val pivot_num: Int = configMap("pivot_num").toInt
  val pp_length: Int = configMap("permutation_prefix_length").toInt

  val block_cap: Int = this.cal_block_cap()
  val block_cap_percent: Float = 0.9F
  val block_min_percent: Float = 0.3F

  //  input file paths
  val dataset_path: String = configMap("dataset_path")
  val init_group_num: Int = Util.getHdfsFileNameList(this.dataset_path).length
  val label_path: String = configMap("label_truth")

  //  output file paths
  val output_prefix: String = dataset_path + "-idx-" +
    this.mode + "-" + this.pivot_num.toString +
    "-" + this.paa_length.toString + "-" + this.pp_length.toString
  val pivot_path: String = output_prefix + "/pivot"
  val pivot_path_raw: String = dataset_path + "_pivots/pivot"

  val shuffle_local: String = output_prefix + "/local_idx"
  val shuffle_path: String = output_prefix + "/data"
  val center_path: String = output_prefix + "/center"
  val g2p_path: String = output_prefix + "/g_2_p"

  //  query configuration
  val query_num: Int = configMap("query_num").toInt
  val knn_K: Int = configMap("knn_k").toInt
  val scope: Float = configMap("scope").toFloat
  val cand_scale: Float = configMap("cand_scale").toFloat
  val min_scale: Float = configMap("min_scale").toFloat
  val fs: Boolean = fetchBoolFromString(configMap("fs"))
  val dataset_query_path: String = configMap("dataset_query_path")

  def get_mode(): String = {
    configMap("mode").toLowerCase match {
      case "group" => "group";
      case _ => "partition"
    }
  }

  val config_index_str: String = {
    ("\n==> Index Configuration" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%.2f" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +

      "\n * %-10s\t%d" +
      "\n * %-10s\t%.2f" +
      "\n * %-10s\t%.2f" +

      "\n * %-10s\t%s" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%s"
      ).format(
      "mode",mode,
      "dsLen",ts_length,
      "ppLen",paa_length,
      "pct",percent,
      "pivotNum",pivot_num,
      "ppLen",pp_length,
      "initGpNum",init_group_num,

      "blkCap",block_cap,
      "blkCapPct",block_cap_percent,
      "blkMinPct",block_min_percent,

      "dataset",dataset_path,
      "pivot",pivot_path,
      "pivotRaw",pivot_path_raw,
      "shufLocal",shuffle_local,
      "shufPath",shuffle_path,
      "centerPath",center_path,
      "g2pPath",g2p_path
    )
  }

  val config_query_str: String = {
    ("\n==> Query Configuration" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +

      "\n * %-10s\t%d" +
      "\n * %-10s\t%d" +
      "\n * %-10s\t%.2f" +
      "\n * %-10s\t%.2f" +
      "\n * %-10s\t%b" +

      "\n * %-10s\t%s" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%s" +
      "\n * %-10s\t%s"
      ).format(
      "mode", mode,
      "paaLen", paa_length,
      "pivotNum", pivot_num,
      "ppLen", pp_length,

      "queryNum", query_num,
      "k-value", knn_K,
      "scope", scope,
      "candMulti", cand_scale,
      "fullScan", fs,

      "dataPath", dataset_query_path,
      "groundTruth", label_path,
      "pivotPath", pivot_path,
      "centerPath", center_path,
      "g2pPath", g2p_path
    )
  }

  private def cal_block_cap(): Int = {
    val record_size = this.ts_length * 4 + 8
    math.ceil(this.blockSize * 1024 * 1024 / record_size).toInt
  }

  def getClass(isPrint: Boolean = false, isIndex: Boolean = true): IdxCfg = {
    if (isPrint) {
      if (isIndex) {
        Util.printLog(config_index_str)
      } else {
        Util.printLog(config_query_str)
      }
    }

    new IdxCfg(
      this.mode,
      this.ts_length,
      this.paa_length,
      this.percent,
      this.pivot_num,
      this.pp_length,
      this.init_group_num,

      this.blockSize,
      this.block_cap,
      this.block_cap_percent,
      this.block_min_percent,

      this.query_num,
      this.knn_K,
      this.scope,
      this.cand_scale,
      this.min_scale,
      this.fs,

      this.dataset_path,
      this.dataset_query_path,
      this.label_path,
      this.pivot_path,
      this.pivot_path_raw,
      this.shuffle_local,
      this.shuffle_path,
      this.center_path,
      this.g2p_path
    )
  }
}

class IdxCfg(val mode: String,
             val ts_length: Int,
             val paa_length: Int,
             val percent: Float,
             val pivot_num: Int,
             val pp_length: Int,
             val init_group_num: Int,

             val block_size: Int,
             val block_cap: Int,
             val block_cap_percent: Float,
             val block_min_percent: Float,

             val query_num: Int,
             val knn_K: Int,
             val scope: Float,
             val cand_scale: Float,
             val min_scale: Float,
             val fs: Boolean,

             val dataset_path: String,
             val dataset_query_path: String,
             val label_path: String,
             val pivot_path: String,
             val pivot_path_raw: String,
             val shuffle_local: String,
             val shuffle_path: String,
             val center_path: String,
             val group_2_partition_path: String
            ) extends Serializable
