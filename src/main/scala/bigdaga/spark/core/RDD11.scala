package bigdaga.spark.core

/**
 * input/agent.log
 * 数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割
 * 需求: 统计出每一个省份广告被点击次数的 TOP3
 */
object RDD11 {

  def main(args: Array[String]): Unit = {
    val sc = getSparkContext(this.getClass.getName)

    val lineRDD = sc.textFile("input/agent.log")
    lineRDD
      .map(line => {
        val splits = line.split(" ")
        ((splits(1), splits(4)), 1)
      })
      .reduceByKey(_ + _)
      .map {
        case ((city, id), cnt) => (city, (id, cnt))
      }
      .groupByKey
      .mapValues(_.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
      .sortBy(_._1) // 按照省份的升序
      .collect
      .foreach(println)
  }
}
