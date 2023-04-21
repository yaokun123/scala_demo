package com.demo.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Api {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")



    val data: RDD[(String, Int)] = sc.parallelize(List(
      ("zhangsan", 234),
      ("zhangsan", 5667),
      ("zhangsan", 343),
      ("lisi", 212),
      ("lisi", 44),
      ("lisi", 33),
      ("wangwu", 535),
      ("wangwu", 22)
    ))
    val sum: RDD[(String, Int)] = data.reduceByKey(_+_)
    val max: RDD[(String, Int)] = data.reduceByKey(  (ov,nv)=> if(ov>nv)  ov else nv     )
    val min: RDD[(String, Int)] = data.reduceByKey(  (ov,nv)=> if(ov<nv)  ov else nv     )
    val count: RDD[(String, Int)] = data.mapValues(e=>1).reduceByKey(_+_)
    val tmp: RDD[(String, (Int, Int))] = sum.join(count)
    val avg: RDD[(String, Int)] = tmp.mapValues(e=> e._1/e._2)

    data.groupByKey().map(x=>(x._1,x._2.toList)).foreach(println)

  }

}
