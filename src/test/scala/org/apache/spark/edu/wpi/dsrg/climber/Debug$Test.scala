package org.apache.spark.edu.wpi.dsrg.climber

import org.apache.spark.edu.wpi.dsrg.climber.cfg.IdxCfg
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable.Set

class Debug$Test extends FunSuite with Logging with BeforeAndAfterAll with Serializable {
  val sc = new SparkContext(new SparkConf()
    .setAppName("TEST")
    .setMaster("local")
    .set("spark.driver.maxResultSize", "2g"))

  test("rank no-order query") {
    val idxcfg = IdxCfg.getClass(false)
    val debug = new Debug
    debug.query_rank_noorder(sc, idxcfg)
  }

  override def afterAll() {
    sc.stop()
  }

  test("paa construction") {
    val idxcfg = IdxCfg.getClass(false)
    val debug = new Debug
    debug.construct_paa(sc, idxcfg.dataset_path)
  }

  test("paa query") {
    val idxcfg = IdxCfg.getClass(false)
    val debug = new Debug
    debug.query_paa(sc, idxcfg)
  }

  test("rank construction") {
    val idxcfg = IdxCfg.getClass(false)
    val debug = new Debug
    debug.construct_rank_noorder(sc, idxcfg)
  }

  test("full pivots index construction"){
    val idxcfg = IdxCfg.getClass(false)
    val debug = new Debug
    debug.construct_full_pivots(sc, idxcfg)
  }

  test("full pivots index query"){
    val idxcfg = IdxCfg.getClass(false)
    val debug = new Debug
    for (len<- 14 to 20 by 2){
      println("prefix len\t", len)
      debug.query_full_pivots(sc, idxcfg, len, len,1000)
    }
  }

  test("counter") {
    val path = "hdfs://localhost:9000/mits/TS-1-100-9"
    val data = sc.objectFile[(Long, Array[Float])](path).map { case (rid, ds) =>
      val pid = TaskContext.getPartitionId()
      (pid, rid)
    }.groupBy(_._1).map { case (pid, rids) =>
      val temp = rids.toArray.map(_._2)
      (pid, temp.min, temp.max, temp.length)
    }.collect()

    for ((pid, min_val, max_val, num) <- data) {
      println(pid, min_val, max_val, num)
    }
  }

  test("set") {
    val k1 = Array(1, 2, 3, 4, 5)
    val k2 = Array(3, 4, 6, 7, 8)

    var s1 = Set[Int]()
    s1 ++= k1
    s1 ++= k2
    println(s1.size)
    println(s1.mkString(","))

    k1.length - (2 * k1.length - s1.size)
    println(s1.size - k1.length)
  }
}