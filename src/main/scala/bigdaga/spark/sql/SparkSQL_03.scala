package bigdaga.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

object SparkSQL_03 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getName)

    // 创建 DF
    val userDf = ss.read.json("input/user.json")
    userDf.show

    userDf.createOrReplaceTempView("user")

    ss.sql("select avg(age) from user").show

    // 注册自定义函数
    ss.udf.register("myAvg", new MyAvg)
    ss.sql("select myAvg(age) from user").show

  }
}

// 用户自定会聚合函数：用户的平均年龄
class MyAvg extends UserDefinedAggregateFunction {
  // 聚合函数输入参数的数据结构
  override def inputSchema: StructType = {
    StructType(StructField("age", LongType) :: Nil)
  }

  // 聚合缓冲区中值的数据结构
  override def bufferSchema: StructType = {
    StructType(StructField("total", LongType) :: StructField("count", LongType) :: Nil)
  }

  // 聚合函数的结构类型
  override def dataType: DataType = DoubleType

  // 确定性: 同样的输入是否返回同样的输出（幂等性）
  override def deterministic: Boolean = true

  // 聚合函数初始化（缓存区初始化）
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 相同 Executor间的合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 不同 Executor 间的合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终的结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}