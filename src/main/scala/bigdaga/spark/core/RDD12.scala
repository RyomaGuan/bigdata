package bigdaga.spark.core

/**
 * input/agent.log
 * 数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割
 * 需求: 统计出每一个省份广告被点击次数的 TOP3
 */
object RDD12 {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val rdd = sc.parallelize(1 to 100)
    val sum: Int = rdd.reduce(_ + _)
  }
}
