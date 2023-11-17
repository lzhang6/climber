package org.apache.spark.edu.wpi.dsrg.climber

import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class RandWalkTsCreater$Test extends FunSuite with Logging with BeforeAndAfterAll with Serializable {
  val sc = new SparkContext(new SparkConf()
    .setAppName("TEST")
    .setMaster("local")
    .set("spark.driver.maxResultSize", "2g"))

  override def afterAll() {
    sc.stop()
  }

  test("creat data") {
    RandWalkTsCreater(sc)
  }

  test("ground truth"){
    GroundTruth(sc)
  }

  test("test "){
    val t_1g = 268008341
    val r = 5 * t_1g/10000
    println(t_1g, r)
  }
}