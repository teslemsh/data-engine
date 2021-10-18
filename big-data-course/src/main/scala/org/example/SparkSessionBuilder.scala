package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkSessionBuilder {

  def getSparkSession(appName: String): SparkSession = {

    val conf = new SparkConf

    //conf.set("spark.master", Properties.envOrElse("SPARK_MASTER_URL", "spark://192.168.1.136:7077"))
    conf.set("spark.submit.deployMode", "cluster")
    conf.set("spark.driver.bindAddress", "0.0.0.0")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.app.name", appName)

    SparkSession
      .builder
      .master("local[*]")
      .config(conf = conf)
      .enableHiveSupport()
      .getOrCreate()

  }
}
