package org.aksw.dice.linda.Factgeneration

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SparkSession, _ }
import scala.collection.mutable
import org.apache.spark.sql.types._

object FactGenerator {
  var originalKB: DataFrame = _
  var newFacts: DataFrame = _
  var rules: DataFrame = _
  val spark = SparkSession.builder
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName("LINDA  (Fact Generator)")
    .getOrCreate()
  def main(args: Array[String]) = {
    val context = spark.sparkContext
    this.rules = spark.read.parquet("/Users/Kunal/workspaceThesis/LINDA/Data/OriginalAlgorithm/NewRules/rdf/parquet");
    this.originalKB = spark.read.parquet("/Users/Kunal/workspaceThesis/LINDA/Data/OriginalAlgorithm/NewRules/rdf/originalKB").withColumn("status", lit("OriginalFact"))
    println(this.originalKB.count())
    // this.rules.show(false)
    this.rules.limit(10).foreach(r => generateFacts(r))
    println(this.originalKB.count())
    spark.stop

  }

  def generateFacts(rule: Row) {
    println("sss")
    val body = rule.getSeq(0)
    val head = rule.getSeq[String](1).toArray
    val confidence = rule.getDouble(2)
    val negation = rule.getString(3)
    def containsBody = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => body.contains(a))
    })
    /* def containsNegation = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => negation.contains(a))
    })*/

    def concatCol = udf((list1: mutable.WrappedArray[String], list2: mutable.WrappedArray[String]) => {
      list1 ++ list2
    })
    this.originalKB = this.originalKB.union(this.originalKB.select(col("subject"))
      .where(containsBody(col("operators")))
      .intersect(this.originalKB.select(col("subject"))
        .where(array_contains(col("operators"), negation)))
      .withColumn("operators", typedLit(head))
      .withColumn("factConf", lit(confidence))
      .withColumn("status", lit("NewFact")))
    println(this.originalKB.count())
    this.originalKB.show(false)

  }
}