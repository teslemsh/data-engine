package org.example

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.nio.file.Paths
import scala.io.Source

object SparkReadJsonTest extends Constants {
  case class Poem(title: String, contents: String)
  def main(args: Array[String]): Unit = {

    val spark = SparkSessionBuilder.getSparkSession("LivyOnDockerApp")
    val df = readJson(spark)

    df.show()
    df.select("Country").distinct().show()
    df.groupBy("Zipcode")
      .sum("TotalWages")
      .as("total")
      .show()

    df.groupBy("Zipcode")
      .max("TotalWages")
      .as("total")
      .show()

    print(df.columns.mkString("Array(", ", ", ")"))


    Thread.sleep(1000000) //For 1000 seconds or more

  }

  def getData(path: String): List[Poem] = {

    new File(path).listFiles.toList.map(f => {

      val basename = Paths
        .get(f.toString)
        .getFileName.toString
        .split("\\.").head

      val source = Source.fromFile(f.toString)
      val contents = source.mkString

      source.close()

      Poem(basename, contents)

    })
  }

  def readJson(spark: SparkSession): DataFrame = {
    val path = getClass.getResource("/file.json").toString
    spark.read.json(path)
  }

  def writeTable(df:DataFrame):DataFrame = {
    df.select(Cols.map(col):_*)
      .write
      .insertInto(TablePath)

    df
  }
}