package bigdaga.spark.sql

/**
 * 统计上传视频最多的用户Top10以及他们上传的观看次数在前20的视频
 */
object SparkSQL_GrainVideo_6 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getSimpleName)

    // 1. t1: 上传视频最多的用户Top10
    ss.sql(
      """
        |select uploader,
        |    videos
        |from user
        |order by videos desc
        |limit 10
        |""".stripMargin)
      .createOrReplaceTempView("t1")

    // 2. t2: t1 关联 video 表, 查看所有人上传的视频
    ss.sql(
      """
        |select videoid,
        |    t1.uploader,
        |    views
        |from t1
        |left join video v on t1.uploader = v.uploader
        |""".stripMargin)
      .createOrReplaceTempView("t2")

    // 3. t3: 开窗 + 排序
    ss.sql(
      """
        |select uploader,
        |    videoid,
        |    views,
        |    rank() over(partition by uploader order by views desc) r
        |from t2
        |""".stripMargin)
      .createOrReplaceTempView("t3")

    // 4. 取前 20
    ss.sql(
      """
        |select uploader,
        |    videoid,
        |    views
        |from t3
        |where r <= 20
        |""".stripMargin)
      .show()
  }
}
