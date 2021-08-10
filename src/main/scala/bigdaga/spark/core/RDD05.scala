package bigdaga.spark.core

object RDD05 {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val rdd = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9), 5)
    // 8,9
    // 1
    // 4,5
    // 6,7
    // 2,3
    rdd.glom.map(_.mkString(",")).foreach(println)

    println(rdd.coalesce(2).partitions.length)

    val rdd1 = rdd.coalesce(2)
    // 4,5,6,7,8,9
    // 1,2,3
    rdd1.glom.map(_.mkString(",")).foreach(println)
  }
}