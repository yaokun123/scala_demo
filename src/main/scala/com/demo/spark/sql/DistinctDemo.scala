package com.demo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object DistinctDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("demo").setMaster("local")

    val ss = new SparkSession.Builder().config(conf).getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    val pro = new Properties ()
    pro.put("url","jdbc:mysql://192.168.109.245/test")
    pro.put("user","root")
    pro.put("password","phpdev")
    pro.put("driver","com.mysql.jdbc.Driver")


    val usersDF: DataFrame = ss.read.jdbc(pro.get("url").toString,"users",pro)

    usersDF.createTempView("users")

    ss.sql("select * from users").show()

    val resDF: DataFrame = ss.sql("select distinct(username) from users")
    resDF.show()

    resDF.write.mode(SaveMode.Append).jdbc(pro.get("url").toString,"users_2",pro)
  }

}
