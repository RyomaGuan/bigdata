package bigdaga.spark.sql

import org.apache.spark.sql.{Encoder, Encoders, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

object SparkSQL_04 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getName)
    import ss.implicits._

    // 创建 DS
    val userDS = ss.read.json("input/user.json").as[Emp]

    // 创建聚合函数
    val udaf = new MyAvgClass
    // 将聚合函数转换为查询列
    val column: TypedColumn[Emp, Double] = udaf.toColumn

    // 使用 DSL 语法
    userDS.select(column).show
  }
}

// DataSet 样例类
case class Emp(username: String, age: Long)

// BUF 样例类
case class AvgBuffer(var total: Long, var count: Long)

// 用户自定会聚合函数(强类型)：用户的平均年龄
// 继承 Aggregator，增加泛型
// abstract class Aggregator[-IN, BUF, OUT] extends Serializable{}
class MyAvgClass extends Aggregator[Emp, AvgBuffer, Double] {
  // 缓存区初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0L, 0L)
  }

  // 相同 Executor间的合并
  override def reduce(buffer: AvgBuffer, emp: Emp): AvgBuffer = {
    buffer.total += emp.age
    buffer.count += 1L
    buffer
  }

  // 不同 Executor 间的合并
  override def merge(buffer1: AvgBuffer, buffer2: AvgBuffer): AvgBuffer = {
    buffer1.total += buffer2.total
    buffer1.count += buffer2.count
    buffer1
  }

  // 计算最终的结果
  override def finish(buffer: AvgBuffer): Double = {
    buffer.total.toDouble / buffer.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}