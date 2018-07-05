package org.aksw.dice.linda.Factgeneration

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, _ }
import scala.collection.mutable
import org.apache.spark.sql.types._
import org.aksw.dice.linda.Utils.LINDAProperties._

object FactGenerator {
  var originalKB: DataFrame = _
  var newFacts: DataFrame = _
  var rules: DataFrame = _
  val spark = SparkSession.builder
    .master(SPARK_SYSTEM)
    .config(SERIALIZER, KYRO_SERIALIZER)
    .config(WAREHOUSE, DIRECTORY)
    .appName(APP_FACT_GENERATOR)
    .getOrCreate()

  def main(args: Array[String]) = {
    val context = spark.sparkContext
    this.rules = spark.read.parquet(EWS_RULES);
    this.originalKB = spark.read.parquet(INPUT_DATASET_SUBJECT_OPERATOR_MAP).withColumn("status", lit("OriginalFact"))
    this.rules.foreach(r => generateFacts(r))
    this.originalKB.write.mode(SaveMode.Overwrite).json(RESULT_KB_EWS)
    spark.stop
  }

  def generateFacts(
    rule: Row): Unit = {
    val body = rule.getSeq(0)
    val head = rule.getSeq[String](1).toArray
    val confidence = rule.getDouble(2)
    val negation = rule.getString(3)
    def containsBody =
      udf((list: mutable.WrappedArray[String]) => {
        list.exists(a => body.contains(a))
      })
    def concatCol =
      udf((list1: mutable.WrappedArray[String],
        list2: mutable.WrappedArray[String]) => {
        list1 ++ list2
      })
    this.originalKB = this.originalKB.union(this.originalKB.select(col("subject"))
      .where(containsBody(col("operators")))
      .intersect(this.originalKB.select(col("subject"))
        .where(array_contains(col("operators"), negation)))
      .withColumn("operators", typedLit(head))
      .withColumn("factConf", lit(confidence))
      .withColumn("status", lit("NewFact")))
  }
}