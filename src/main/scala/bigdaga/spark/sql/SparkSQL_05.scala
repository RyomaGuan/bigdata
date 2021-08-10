package bigdaga.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

object SparkSQL_05 {
  def main(args: Array[String]): Unit = {
    val ss = getSparkSession(this.getClass.getName)
    import ss.implicits._
    val user_visit_action = loadCSV(ss, "input/user_visit_action.txt")
    user_visit_action.createOrReplaceTempView("user_visit_action")

    val city_info = loadCSV(ss, "input/city_info.txt")
    city_info.createOrReplaceTempView("city_info")

    val product_info = loadCSV(ss, "input/product_info.txt")
    product_info.createOrReplaceTempView("product_info")

    val t1 = ss.sql(
      """
        |select c.city_name,
        |    c.area,
        |    p.product_name
        |from user_visit_action u
        |join product_info p on u.click_product_id = p.product_id
        |join city_info c on u.city_id = c.city_id
        |where u.click_product_id != -1
        |""".stripMargin)

    t1.show(20)
    t1.createOrReplaceTempView("t1")

    ss.udf.register("cityRemark", new CityRemark)
    val t2 = ss.sql(
      """
        |select area,
        |    product_name,
        |    count(*) cnt,
        |    cityRemark(city_name) cityRemark
        |from t1
        |group by area, product_name
        |""".stripMargin)

    writeCSV(t2, "output/cit_count_top3")
  }
}

class CityRemark extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("city_name", StringType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("totalCount", LongType)
      :: StructField("cityCount", MapType(StringType, LongType))
      :: Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 1L
    buffer(1) = Map[String, Long]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + 1L

    val cityName = input.getString(0)
    val cityCount = buffer.getAs[Map[String, Long]](1)
    buffer(1) = cityCount + (cityName -> (cityCount.getOrElse(cityName, 0L) + 1L))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    val cityCount1 = buffer1.getAs[Map[String, Long]](1)
    val cityCount2 = buffer2.getAs[Map[String, Long]](1)

    buffer1(1) = cityCount1.foldLeft(cityCount2) {
      case (map, (city, count)) => map + (city -> (map.getOrElse(city, 0L) + count))
    }
  }

  override def evaluate(buffer: Row) = {
    val totalCount = buffer.getLong(0)
    val cityCount = buffer.getAs[Map[String, Long]](1)

    var cityCountTop = cityCount.toList
      .sortBy(_._2)(Ordering.Long.reverse)
      .take(2)

    if (cityCount.size > 2) {
      val remainder = totalCount - cityCountTop.map(_._2).sum
      cityCountTop = cityCountTop :+ ("其他", remainder)
    }

    cityCountTop.map {
      case (cityName, count) => cityName + ":" + (count.toDouble / totalCount * 100).formatted("%.2f") + "%"
    }
      .mkString(", ")
  }
}

