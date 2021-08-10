package bigdaga.spark.sql

object SparkSQL_02 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getName)

    // 创建 DF
    val df = ss.read.json("input/user.json")
    df.show

    // 注册一个 udf 函数: toUpper是函数名, 第二个参数是函数的具体实现
    ss.udf.register("toUpper", (s: String) => s.toUpperCase)

    df.createOrReplaceTempView("user")
    ss.sql(
      """
        |select toUpper(username), age
        |from user
        |""".stripMargin
    ).show
  }
}
