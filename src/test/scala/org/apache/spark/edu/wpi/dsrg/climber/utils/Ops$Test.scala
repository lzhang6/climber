package org.apache.spark.edu.wpi.dsrg.climber.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class Ops$Test extends FunSuite {

  //  test("min heap") {
  //    val minHeap = scala.collection.mutable.PriorityQueue.empty(Ordering.by[(Float, Short)](_._1).reverse)
  //
  //    println("Inserting 4,2,3")
  //    minHeap += 4
  //    minHeap += 2
  //    minHeap += 3
  //
  //    println("at the minimuns are")
  //    println(minHeap.dequeue)
  //    println(minHeap.dequeue)
  //    println(minHeap.dequeue)
  //  }

  test("2") {
    import scala.util.Sorting
    val pairs = Array(("a", 5, 2), ("c", 3, 1), ("b", 1, 3))
    Sorting.quickSort(pairs)(Ordering[(Int, String)].on(x => (x._3, x._1)))
    println(pairs.mkString(","))
  }

  test("5") {
    val sc = new SparkContext(new SparkConf()
      .setAppName("TEST")
      .setMaster("local")
      .set("spark.driver.maxResultSize", "2g"))

    val path = "hdfs://localhost:9000/mits/TS-1-100-9-debug/pivots_200"
    val pivots = sc.objectFile[(Short, Array[Float])](path).collect()

    val data = pivots(10)._2

    val p1 = Ops.cvtPaaTo4Ps_os(data, pivots, 8)
    val p2 = Ops.cvtPaaTo4Ps_os_whole(data, pivots)

    println(p1.mkString(","))
    println(p2.mkString(","))
  }

  test("distance overlap") {

    val a = Array(1, 2, 3, 4, 5, 6, 7).map(_.toShort)
    val b = Array(7, 8, 9, 10, 11, 12, 13).map(_.toShort)
    val c = Array(1, 2, 4, 7, 8, 15, 16).map(_.toShort)
    val d = Array(1, 2, 7, 8, 9, 15, 16).map(_.toShort)
    val e = Array(7, 15, 16, 17, 18, 19, 20).map(_.toShort)

    println(Util.compute_overlap_dist(a, b))
    println(Util.compute_overlap_dist(a, c))
    println(Util.compute_overlap_dist(b, c))
    println(Util.compute_overlap_dist(a, d))
    println(Util.compute_overlap_dist(b, d))
    println(Util.compute_overlap_dist(a, e))
    println(Util.compute_overlap_dist(b, e))

  }
}