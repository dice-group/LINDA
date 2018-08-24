package org.aksw.dice.linda.Utils
import org.aksw.dice.linda.Utils.LINDAProperties._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object FactFilterForLPEval {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .appName("Fact Filter")
      .getOrCreate()

    if (args.length == 0) {
      println("No Parameters provided")
      spark.stop()
    }
    var Originaldataset = args(0)
    var NewFacts = args(1)

    val original = spark.createDataFrame(spark.sparkContext.textFile(Originaldataset)
      .filter(!_.startsWith("#")).map(data => TripleUtils.parsTriples(data)))
      .withColumn(
        "triple",
        concat(col("subject"), lit(" "), col("predicate"), lit(" "), col("object")))
      .drop("subject")
      .drop("predicate")
      .drop("object")
    val newFacts = spark.read.json(NewFacts)

    println("Original Rule Count " + original.count())
    println("Number of New facts " + newFacts.count())
    newFacts
      .join(original, Seq("triple"), "left_anti")
      .show(false)

  }
}