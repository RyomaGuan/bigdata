package bigdaga.spark.sql

/**
 * 统计视频类别热度Top10(类别热度按照类别的总播放量)
 */
object SparkSQL_GrainVideo_2 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getSimpleName)

    // t1: video explode category
    ss.sql(
      """
        |select videoId,
        |    views,
        |    categories
        |from video
        |lateral view explode(category) tbl as categories
        |""".stripMargin)
      .createOrReplaceTempView("t1")

    ss.sql(
      """
        |select categories,
        |    sum(views) hot
        |from t1
        |group by categories
        |order by hot desc
        |limit 10
        |""".stripMargin)
      .show()
  }
}
