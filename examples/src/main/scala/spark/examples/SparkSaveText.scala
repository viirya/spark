package spark.examples

import spark._
import SparkContext._

import org.apache.hadoop.conf.Configuration


object SparkSaveText {
  def main(args: Array[String]) {
    val spark = new SparkContext(args(0), "SparkSaveText")
    val numbers = spark.parallelize(1 to 100)
    val namenode =  new Configuration().get("fs.default.name")

    numbers.saveAsTextFile(namenode + "/user/hduser/numbers")

    spark.makeRDD(List("Pi is 3.14 "), 1).saveAsTextFile(namenode + "/user/hduser/pi")

    spark.stop()
  }
}
