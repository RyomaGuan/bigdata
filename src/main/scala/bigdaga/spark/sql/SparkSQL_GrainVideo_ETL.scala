package bigdaga.spark.sql

import java.io.{File, PrintWriter}
import scala.io.Source


object SparkSQL_GrainVideo_ETL {
  private val videoDir = new File("dataset/grain_video/video")

  def main(args: Array[String]): Unit = {
    for (file <- videoDir.listFiles.filter(_.isFile)) {
      println(file)
      val reader = Source.fromFile(file)
      val lines = reader.getLines
        .map(_.split("\t"))
        .filter(_.length >= 8)
        .map {
          array => {
            array(3) = array(3).replace(" ", "")
            array.take(9).mkString("\t") + "\t" + array.drop(9).mkString("&")
          }
        }
        .toList

      val writer = new PrintWriter(file)
      lines.foreach(writer.println)
    }
  }
}

