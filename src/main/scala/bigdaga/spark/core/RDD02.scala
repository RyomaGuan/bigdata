package bigdaga.spark.core

object RDD02 {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    rdd.mapPartitionsWithIndex((idx, item) => item.map((idx, _))).collect.foreach(println)
    println(rdd.mapPartitionsWithIndex((idx, item) => item.map((idx, _))).filter(_._1 == 2).map(_._2).sum)
  }
}