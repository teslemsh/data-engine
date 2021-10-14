package org.example

trait Constants {
  val TablePath = "data.google"
  val Bbdd: String =
    """
      |CREATE DATABASE IF NOT EXISTS data
      |""".stripMargin

  val TableSchema: String =
    """ CREATE TABLE IF NOT EXISTS data.google (
      |    date string,
      |    open string,
      |    high string,
      |    low string,
      |    close string,
      |    ddj_lose string,
      |    volue string)
      |    STORED AS PARQUET
      |""".stripMargin

  val Cols = Seq(
    "date",
    "open",
    "high",
    "low",
    "close",
    "ddj_lose",
    "volue"
  )

}
