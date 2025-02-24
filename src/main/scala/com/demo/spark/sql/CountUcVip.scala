package com.demo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object CountUcVip {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("demo").setMaster("local")

    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = session.sparkContext
    sc.setLogLevel("ERROR")


    val value: RDD[String] = sc.textFile("data/uc_new_3.log").map(x => {
      val strings: Array[String] = x.split("信息：")
      strings(1)
    })

    val frame: DataFrame = session.read.json(value)

    // frame.printSchema()
    frame.createTempView("demo")

    // session.sql("select count(*) from demo").show()
    // session.sql("select sum(anVipSize),sum(iosMinipVipPhoneSize),sum(iosGzhVipPhoneSize) from demo").show()
    // session.sql("select DATE_FORMAT(date,'yyyy-MM'),sum(anVipSize+iosMinipVipPhoneSize+iosGzhVipPhoneSize) from demo  group by DATE_FORMAT(date,'yyyy-MM') order by DATE_FORMAT(date,'yyyy-MM') asc").show()
    // session.sql("select DATE_FORMAT(date,'yyyy-MM-dd'),intoUv from demo order by DATE_FORMAT(date,'yyyy-MM-dd') asc").coalesce(1).write.option("header", "true").csv("/tmp/demo_1")
    session.sql("select anVipPhones from demo limit 2;").show()

    // session.createDataFrame()
  }

}
