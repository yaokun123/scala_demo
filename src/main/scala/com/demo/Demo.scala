package com.demo

object Demo {
  def main(args: Array[String]): Unit = {
    val inclusive = 1 to (10)
    for (i <- inclusive){
      println(i)
    }

    // val ints = for (i <- 1 to (10)) yield i
  }

}
