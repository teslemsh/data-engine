package org.example

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object SparkSessionTest extends Constants {

  def main(args:Array[String]): Unit ={

   val spark = SparkSession.builder()
      .master("local[5]")
      .appName("SparkByExample")
      .enableHiveSupport()
      .getOrCreate();

    println("First SparkContext:")
    println("APP Name :"+spark.sparkContext.appName);
    println("Deploy Mode :"+spark.sparkContext.deployMode);
    println("Master :"+spark.sparkContext.master);

    createBBDD(spark)
    createTable(spark)

    val df = readTable(spark)
    val df2 = renameCols(df)
    writeTable(df2)


    Thread.sleep(1000000) //For 1000 seconds or more

  }

  def readTable(spark: SparkSession): DataFrame = {
    val path = getClass.getResource("/google.csv").toString
    spark.read.option( "header", value = true ).csv(path)
  }

  def createBBDD(spark: SparkSession): DataFrame = {
    import spark.sql

    sql(Bbdd)
  }
  def createTable(spark: SparkSession): DataFrame = {
    import spark.sql

    sql(TableSchema)
  }

  def writeTable(df:DataFrame):DataFrame = {
    df.select(Cols.map(col):_*)
      .write
      .insertInto(TablePath)

    df
  }

  def renameCols(df: DataFrame): DataFrame = {
    df
      .withColumnRenamed("Adj Close", "ddj_lose")
      .withColumnRenamed("Close", "close")
      .withColumnRenamed("Date", "date")
      .withColumnRenamed("High", "high")
      .withColumnRenamed("Low", "low")
      .withColumnRenamed("Open", "open")
      .withColumnRenamed("Volume", "volume")
      .withColumn("volue", lit("volue"))
  }
}