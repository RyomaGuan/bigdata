package bigdaga.spark.core

object RDD04 {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val rdd = sc.makeRDD(Array(1, 1, 1, 2, 2, 3, 3, 3, 3), 2)
    rdd.distinct.collect.foreach(println)
    rdd.map((_, null)).reduceByKey((x, y) => x, 2).collect.foreach(println)
    rdd.map((_, null)).reduceByKey((x, y) => x, 2).map(_._1).collect.foreach(println)
  }
}