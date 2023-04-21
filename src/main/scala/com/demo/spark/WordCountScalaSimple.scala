package com.demo.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountScalaSimple {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("spark-scala-simple")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val fileFDD: RDD[String] = sc.textFile("data/testdata.txt")

    // fileFDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
    // fileFDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).map(x=>{(x._2,1)}).foreach(println)

    // Thread.sleep(Integer.MAX_VALUE)
  }

}
