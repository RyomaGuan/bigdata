package bigdaga.spark.sql

/**
 * 统计出视频观看数最高的20个视频的所属类别以及类别包含Top20视频的个数
 */
object SparkSQL_GrainVideo_3 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getSimpleName)
    // t1: 统计视频观看数Top20
    ss.sql(
      """
        |select videoId,
        |    views,
        |    category
        |from video
        |order by views desc
        |limit 20
        |""".stripMargin)
      .createOrReplaceTempView("t1")

    // t2: t1 explode category
    ss.sql(
      """
        |select videoId,
        |    categories
        |from t1
        |lateral view explode(category) tbl as categories
        |""".stripMargin)
      .createOrReplaceTempView("t2")

    // 统计个数
    ss.sql(
      """
        |select categories,
        |    count(1) cnt
        |from t2
        |group by categories
        |""".stripMargin)
      .show()
  }
}
