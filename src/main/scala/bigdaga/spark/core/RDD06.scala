package bigdaga.spark.core

object RDD06 {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val rdd1 = sc.parallelize(Array(1,3,4,10,4,6,9,20,30,16))
    println(rdd1.sortBy(x=>x).collect.mkString(","))
  }
}