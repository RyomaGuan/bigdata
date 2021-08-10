package bigdaga.spark.sql

/**
 * 统计视频观看数Top10
 */
object SparkSQL_GrainVideo_1 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getSimpleName)
    ss.sql(
      """
        |select videoId,
        |    views,
        |    category
        |from video
        |order by views desc
        |limit 10
        |""".stripMargin)
      .show()
  }
}
