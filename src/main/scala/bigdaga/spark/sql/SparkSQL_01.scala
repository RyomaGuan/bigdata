package bigdaga.spark.sql

object SparkSQL_01 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getName)
    // 当 SparkSQL 需要进行转换是，必须导入隐式转换。这里的 ss 不是包名，是 SparkSession 对象名称
    import ss.implicits._

    // 创建 DF
    val df = ss.read.json("input/user.json")
    df.show

    // 创建 DS
    val users = Seq(User(1, "zhang", 30), User(2, "li", 40))
    val ds = users.toDS
    ds.show

    val rdd = ss.sparkContext.makeRDD(List((3, "wan", 50)))
    // RDD -> DF
    val df1 = rdd.toDF("id", "name", "age")
    // DF -> RDD
    val rdd1 = df1.rdd

    // RDD -> DS
    val userRDD = rdd.map(t => User(t._1, t._2, t._3))
    val ds1 = userRDD.toDS
    // DS -> RDD
    val rdd2 = ds1.rdd

    // DF -> DS
    val ds2 = df1.as[User]
    // DS -> DF
    val df2 = ds2.toDF


    // 使用 SQL 语法访问数据
    df.createOrReplaceTempView("user")
    ss.sql(
      """
        |select * from user
        |""".stripMargin
    ).show

    // 使用 DSL 语法访问数据
    df.select("age").show
  }
}

case class User(id: Int, name: String, age: Int)