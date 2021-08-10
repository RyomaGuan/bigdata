package bigdaga.spark.core

object WordCount {
  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val wordAndCount = sc.textFile("input")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect
    wordAndCount.foreach(println)
  }
}