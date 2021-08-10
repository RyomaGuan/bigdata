package bigdaga.spark.core

object RDD08 {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val rdd = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 1), ("a", 1), ("a", 1), ("b", 2), ("b", 2), ("b", 2), ("b", 2)))
    val collect = rdd.reduceByKey(_ + _).collect
  }
}