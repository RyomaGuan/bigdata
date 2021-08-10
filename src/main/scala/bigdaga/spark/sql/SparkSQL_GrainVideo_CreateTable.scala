package bigdaga.spark.sql

object SparkSQL_GrainVideo_CreateTable {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getSimpleName)

    // 创建 user 表, 并导入数据
    ss.sql(
      """
        |create table if not exists video(
        |    videoId string,
        |    uploader string,
        |    age int,
        |    category array<string>,
        |    length int,
        |    views int,
        |    rate float,
        |    ratings int,
        |    comments int,
        |    relatedId array<string>)
        |row format delimited fields terminated by "\t"
        |collection items terminated by "&"
        |""".stripMargin)
    println("create video table")
    ss.sql(
      """
        |load data local inpath "dataset/grain_video/video" overwrite into table video
        |""".stripMargin)
    println("load data into video table")

    // 创建 video 表, 并导入数据
    ss.sql(
      """
        |create table if not exists  user(
        |    uploader string,
        |    videos int,
        |    friends int)
        |row format delimited fields terminated by "\t"
        |""".stripMargin)
    println("create user table")

    ss.sql(
      """
        |load data local inpath "dataset/grain_video/user.txt" overwrite into table user
        |""".stripMargin)
    println("load data into user table")
  }
}
