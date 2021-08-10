package bigdaga.spark.core

object RDD01 {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), 3)
    rdd.saveAsTextFile("output")
  }
}