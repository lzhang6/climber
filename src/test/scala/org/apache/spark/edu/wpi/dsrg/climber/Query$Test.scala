package org.apache.spark.edu.wpi.dsrg.climber

import org.apache.spark.edu.wpi.dsrg.climber.cfg.IdxCfg
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.edu.wpi.dsrg.climber.idx.{Compress_Structure, Pivot_2_Group, Pivots, Group}
import org.apache.spark.edu.wpi.dsrg.climber.utils.{Evaluate, Ops}
import org.apache.spark.edu.wpi.dsrg.climber.utils.Util.printLog

class Query$Test extends FunSuite with Logging with BeforeAndAfterAll with Serializable {

  val sc = new SparkContext(new SparkConf()
    .setAppName("TEST")
    .setMaster("local")
    .set("spark.driver.maxResultSize", "2g"))

  override def afterAll() {
    sc.stop()
  }

  test("ground truth"){
    val path = "hdfs://localhost:9000/mits/TS-500000-3072-49-gt/gt-20-500-3072-true"
    val data = sc.objectFile[(Array[Float], Array[Float])](path)
      .map { case (query, label) => (query, label.take(100)) }
      .take(100)

    for (k <- data) {
      val a =  k._1.max
      val b = k._1.min
      println(Float.MaxValue - math.pow(a - b, 2) * 25000)
    }
  }

  test("print format"){
//    println(IdxCfg.config_index_str)
    println(IdxCfg.config_query_str)
  }


//  test("debug - 2"){
//    val idxcfg = IdxCfg.getClass(false, false)
//    val q = new Query()
//
//    val query_with_groundTruth = q.load_ground_truth(sc, idxcfg)
//    val pivots_bc = Pivots.read_pivots_bc(sc, idxcfg)
//    val comp_struct = Compress_Structure(sc, idxcfg)
//
//    val centers = sc.objectFile[(Int, Array[Short])](idxcfg.center_path).collect()
//    val pivot_2_gids_mp = Group.cvt_centers_2_pivot_2_gids(sc, centers)
//    val p2g = Pivot_2_Group(centers = centers, pivot_2_gids = pivot_2_gids_mp)
//
//    for (((query, dist_label), idx) <- query_with_groundTruth.zipWithIndex) {
//      if (idx == 7){
//        val paa = Ops.cvtTsToPaa(query, idxcfg.paa_length)
//        val p4s_os_str = Ops.cvtPaaTo4PsOs(paa, pivots_bc.value, idxcfg.pp_length)
//        val p4s_os = p4s_os_str.split(":").map(_.toShort)
//
//        println(p4s_os_str, p4s_os.mkString(","))
//        val t = comp_struct.search(p4s_os, idxcfg)
//        println("compact struct search\t", t.mkString(","))
//
//        val t2 = p2g.get_gid(p4s_os_str)
//        println("p2g\t", t2)
//      }
//    }
//  }
//
//  test("debug") {
//    val idxcfg = IdxCfg.getClass(false, false)
//    val q = new Query()
//
//    val query_with_groundTruth = q.load_ground_truth(sc, idxcfg)
//    val pivots_bc = Pivots.read_pivots_bc(sc, idxcfg)
//    val comp_struct = Compress_Structure(sc, idxcfg)
//
//    for (((query, dist_label), idx) <- query_with_groundTruth.zipWithIndex) {
//      if (idx == 7){
//        val (dist_pred, candidate_size, partition_size) = q.process_one_query(sc, query, pivots_bc.value,
//          comp_struct, idxcfg)
//
//        val recall = Evaluate.callRecall(dist_pred, dist_label)
//        //      val avg_prec = Evaluate.calAveragePrecision(dist_pred, dist_label)
//        val error = Evaluate.calErrorRatio(dist_pred, dist_label)
//
//        printLog("--> [%d/%d] recall: %.3f, error: %.3f, cand_size: %d\n".format(idx + 1,
//          idxcfg.query_num, recall, error, candidate_size))
//      }
//    }
//  }
}
