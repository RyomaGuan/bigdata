package bigdaga.spark.core

object RDD07 {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val rdd = sc.parallelize(Array("hello", "scala", "hello", "spark"))
    rdd.groupBy(x=>x).map{
      case (word, iter) => (word, iter.size)
    }.collect.foreach(println)
  }
}