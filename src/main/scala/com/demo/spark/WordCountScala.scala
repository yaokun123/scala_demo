package com.demo.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("wordcount")
    conf.setMaster("local") // 单机本地运行

    val sc = new SparkContext(conf)
    val fileRDD: RDD[String] = sc.textFile("data/testdata.txt")

    // hello world
    val words: RDD[String] = fileRDD.flatMap((x: String) => {
      x.split(" ")
    })

    // hello
    // world
    val pairWord: RDD[(String, Int)] = words.map((x: String) => {
      new Tuple2(x, 1)
    })

    // (hello, 1)
    // (world, 1)
    // (hello, 1)
    val res: RDD[(String, Int)] = pairWord.reduceByKey((x: Int, y: Int) => {
      // x: OldValue y: Value
      x + y
    })

    res.foreach(println)
  }

}
