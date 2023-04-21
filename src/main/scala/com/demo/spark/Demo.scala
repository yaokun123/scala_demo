package com.demo.spark

import com.google.gson.{JsonElement, JsonObject, JsonParser}
import org.apache.spark.{SparkConf, SparkContext}

object Demo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("demo").setMaster("local")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val fileRDD = sc.textFile("data/demo.log")
    val mapRDD = fileRDD.map(x => {
      val lists: Array[String] = x.split("信息：")
      val jsonStr = lists(1)

      val parser = new JsonParser()
      val obj = parser.parse(jsonStr).asInstanceOf[JsonObject]
      val date = obj.get("date")
      val anVipSize: Int = obj.get("anVipSize").getAsInt
      val iosMinipVipPhoneSize = obj.get("iosMinipVipPhoneSize").getAsInt
      val iosGzhVipPhoneSize = obj.get("iosGzhVipPhoneSize").getAsInt
      (date, anVipSize+iosMinipVipPhoneSize+iosGzhVipPhoneSize)
    })

    mapRDD.foreach(println)


    //mapRDD.cache()
    //mapRDD.persist(StorageLevel.MEMORY_ONLY)


    mapRDD.map(x=>{("A", x._2)}).reduceByKey(_+_).foreach(println)
    // mapRDD.map(x=>{("A", x._2)}).reduceByKey(_+_).saveAsTextFile("/tmp/demo_result")

  }

}
