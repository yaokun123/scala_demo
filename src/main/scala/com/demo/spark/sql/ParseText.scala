package com.demo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ParseText {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("parse text").setMaster("local")

    val ss = SparkSession.builder().config(conf).getOrCreate()
    val sc = ss.sparkContext
    sc.setLogLevel("ERROR")

    // val fileDF: DataFrame = ss.read.text("data/test.log")


    import ss.implicits._
    val fileDS: Dataset[String] = ss.read.textFile("data/test.log")
    val person: Dataset[(String, Int)] = fileDS.map(
      line => {
        val strs: Array[String] = line.split(" ")
        (strs(0), strs(1).toInt)
      }
    )
    val fileDF: DataFrame = person.toDF("name", "age")

    fileDF.printSchema()

    fileDF.createTempView("person")

    ss.sql("select * from person").show()
    ss.sql("select * from person where age>20").show()
  }

}
