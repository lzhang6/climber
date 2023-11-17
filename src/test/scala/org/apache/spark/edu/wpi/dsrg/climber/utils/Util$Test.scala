package org.apache.spark.edu.wpi.dsrg.climber.utils

import scala.util.Random
import org.scalatest.FunSuite

class Util$Test extends FunSuite {
  test("array to maps"){
    val k = Array(3,1,4,6)

    val k2 = k.zipWithIndex.toMap
    println(k2)
    k2.getOrElse(5, -1)
  }

  test("loop"){
    for (a_idx <- 0 until 5){
      for (b_idx <- a_idx + 1 until 5){
        println(a_idx, b_idx)
      }
    }
  }

//  test("compute_kendall_tau"){
//    val a = Array(10,5,6,1).map(_.toShort)
//    val b = Array(10,5,1,8).map(_.toShort)
//
//    val r = Util.compute_kendall_tau(a, b, 4)
//    println(r)
//  }

  test("compute_kendall_tau_2"){
    val p = (1 to 10).toList.map(_.toShort)
    val a = Random.shuffle(p).toArray
    val b = Random.shuffle(p).toArray

    println(a.mkString(" "))
    println(b.mkString(" "))

    val r = Util.compute_kendall_tau2(a, b, 5)
    println(r)
  }
}
