package org.apache.spark.edu.wpi.dsrg.climber.idx

import org.apache.spark.edu.wpi.dsrg.climber.utils.Util

case class TrieNode(pivots: String,
                    var rcd_num: Int = -1,
                    var children: Map[Short, TrieNode] = Map(),
                    var pid: Int = -1) extends Serializable {
  def set_rcd_num(input_rcd_num: Int): Unit = {
    this.rcd_num = input_rcd_num
  }

  def insert_node(node: TrieNode): Unit = {
    if (node.level > this.level + 1) {
      val next_pivot = node.pivots.split(":")(this.level).toShort
      this.children.get(next_pivot) match {
        case Some(child) => child.insert_node(node);
        case None => Util.printLog("ERROR: insert [%s] into [%s], %d".format(node.pivots, this.pivots, next_pivot))
      }
    } else {
      val next_pivot = node.pivots.split(":").last.toShort
      this.children += (next_pivot -> node)
    }
  }

  def update_pid(node: (String, Int), pid: Int): Unit = {
    val node_level = node._1.split(":").length
    if (node_level > this.level) {
      val next_pivot = node._1.split(":")(this.level).toShort
      this.children.get(next_pivot) match {
        case Some(child) => child.update_pid(node, pid);
        case None => Util.printLog("ERROR: update pid [%s] in [%s]'s children, %d".format(node._1, this.pivots,
          next_pivot))
      }
    } else {
      if (node._1 == this.pivots) {
        this.pid = pid
      } else {
        Util.printLog("ERROR: update pid [%s] for [%s]".format(node._1, this.pivots))
      }
    }
  }

  def assign_pid(p4s: String): Int = {
    if (this.pid == -1) {
      val next_pivot = p4s.split(":")(this.level).toShort
      this.children.get(next_pivot) match {
        case Some(child) => child.assign_pid(p4s);
        case None => -1;
      }
    } else {
      this.pid
    }
  }

  def level: Int = {
    if (pivots == "root") {
      0
    } else {
      pivots.split(":").length
    }
  }

  override def toString: String = {
    var output = " " * this.level + "|--" + this.pivots + " rcd_num: " + this.rcd_num.toString + " pid: " + this.pid
      .toString

    for (child <- this.children.toArray) {
      output = output + "\n" + child._2.toString()
    }

    output
  }
}