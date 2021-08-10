package bigdaga.spark.core

object RDD10 {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val rdd1 = sc.parallelize(Array((1, 10), (2, 20), (1, 100), (3, 30)))
    val rdd2 = sc.parallelize(Array((1, "a"), (2, "b"), (1, "aa"), (3, "c")))
    rdd1.cogroup(rdd2).collect
  }
}