package org.aksw.dice.linda.evalutaion
import org.aksw.dice.linda.Utils.LINDAProperties._
import org.apache.spark.sql.SparkSession
object Evaluation {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .appName(APP_EWS_MINER)
      .getOrCreate()
    println(spark.read.json("/Users/Kunal/results/wordnet_rules.json").count())
  }
}