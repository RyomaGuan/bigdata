package bigdaga.spark.sql

object SparkSQL_GrainVideo_4 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getSimpleName)
    // t1: 统计视频观看数Top50所关联视频
    ss.sql(
      """
        |select relatedId,
        |    category
        |from video
        |order by views desc
        |limit 50
        |""".stripMargin)
      .createOrReplaceTempView("t1")

    // t2: explode(relatedId), 可能含重复 relatedId
    ss.sql(
      """
        |select explode(relatedId) vid
        |from t1
        |""".stripMargin)
      .createOrReplaceTempView("t2")

    // t3: t2 关联原表找类别
    ss.sql(
      """
        |select videoId,
        |    category
        |from t2
        |left join video v on t2.vid = v.videoId
        |""".stripMargin)
      .createOrReplaceTempView("t3")

    // t4: explode t3 category
    ss.sql(
      """
        |select videoId,
        |    categories
        |from t3
        |lateral view explode(category) tbl as categories
        |""".stripMargin)
      .createOrReplaceTempView("t4")

    // 统计 categories 个数, 并且排名
    ss.sql(
      """
        |select categories,
        |    count(1) cnt
        |from t4
        |group by categories
        |order by cnt desc
        |""".stripMargin)
      .show()
  }
}
