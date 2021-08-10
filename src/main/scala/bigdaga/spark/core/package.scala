package bigdaga.spark

import org.apache.spark.{SparkConf, SparkContext}

package object core {
  def getSparkContext(name: String) = {
    val sparkConf = new SparkConf().setAppName(name).setMaster("local[*]")
    new SparkContext(sparkConf)
  }
}
