package bigdaga.spark.sql

/**
 * 统计每个类别中的视频热度Top10，以Music为例
 */
object SparkSQL_GrainVideo_5 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getSimpleName)

    // t1: explode category
    ss.sql(
      """
        |select videoId,
        |    views,
        |    categories
        |from video
        |lateral view explode(category) tbl as categories
        |""".stripMargin)
      .createOrReplaceTempView("t1")

    // Music 视频热度Top10
    ss.sql(
      """
        |select videoId,
        |    categories,
        |    views
        |from t1
        |where categories = "Music"
        |order by views desc
        |limit 10
        |""".stripMargin)
      .show()
  }
}
