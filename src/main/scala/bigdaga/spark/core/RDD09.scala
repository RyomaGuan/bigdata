package bigdaga.spark.core

import org.apache.spark.rdd.RDD

object RDD09 {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val rdd = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)
    // acc 累加器, 用来记录分区内的值的和这个 key 出现的次数
    // acc1, acc2 跨分区的累加器
    val combineByKeyRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (_, 1), // 88 => (88, 1)
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), // (88, 1) + 91 => (179, 2)
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    // Array((b,95), (a,91))
    combineByKeyRDD.map {
      case (key, (sum, size)) => (key, sum / size)
    }.collect
  }
}