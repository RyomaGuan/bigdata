package bigdaga.spark.sql

/**
 * 统计每个类别视频观看数Top10
 */
object SparkSQL_GrainVideo_7 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getSimpleName)

    // 1. t1: explode category
    ss.sql(
      """
        |select videoId,
        |    views,
        |    categories
        |from video
        |lateral view explode(category) tbl as categories
        |""".stripMargin)
      .createOrReplaceTempView("t1")

    // t2: 开窗 + 排序
    ss.sql(
      """
        |select videoid,
        |    views,
        |    categories,
        |    rank() over(partition by  categories order by views desc) r
        |from t1
        |""".stripMargin)
      .createOrReplaceTempView("t2")

    // 取前 10
    ss.sql(
      """
        |select videoid,
        |    views,
        |    categories
        |from t2
        |where r <= 10
        |""".stripMargin)
      .show()
  }
}
