package org.apache.spark.edu.wpi.dsrg.climber

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.edu.wpi.dsrg.climber.cfg.IdxCfg

class Index$Test extends FunSuite with Logging with BeforeAndAfterAll with Serializable {

  val sc = new SparkContext(new SparkConf()
    .setAppName("TEST")
    .setMaster("local")
    .set("spark.driver.maxResultSize", "2g"))

  override def afterAll() {
    sc.stop()
  }

  test("index") {
    Index(sc)
  }

  test("query") {
    Query(sc)
  }
}