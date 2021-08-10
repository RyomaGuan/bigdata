package bigdaga.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

package object sql {
  def getSparkSession(name: String) = SparkSession.builder
    .appName(name)
    .config("spark.driver.host", "localhost")
    .enableHiveSupport
    .master("local[*]")
    .getOrCreate

  def loadCSV(ss: SparkSession, path: String, sep: String = "\t") = ss.read
    .format("csv")
    .option("sep", sep)
    .option("header", "true")
    .option("multiLine", "true")
    .load(path)

  def writeCSV(df: DataFrame, path: String, mode: String = "overwrite", delimiter: String = "\t"): Unit = df.repartition(1)
    .write
    .format("csv")
    .mode(mode)
    .option("header", "true")
    .option("delimiter", delimiter)
    .save(path)
}
