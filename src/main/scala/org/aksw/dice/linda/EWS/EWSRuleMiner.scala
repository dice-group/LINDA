package org.aksw.dice.linda.EWS

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.aksw.dice.linda.Utils.LINDAProperties._
import org.apache.spark.sql.expressions.Window;
import org.slf4j.LoggerFactory
import org.apache.spark.ml.fpm.FPGrowth
import scala.collection.mutable
import org.aksw.dice.linda.Utils.TripleUtils
import org.apache.spark.sql.SaveMode

object EWSRuleMiner {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .appName(APP_EWS_MINER)
      .getOrCreate()

    if (args.length == 0) {
      println("No Parameters provided")
      spark.stop()
    }

    var DATASET_NAME = args(0)
    var HDFS_MASTER = args(1)

    var EWS_FACTS_WITH_RULES = HDFS_MASTER + "EWS/" + DATASET_NAME + "/EWSfactswithRules/"
    var EWS_RULES = HDFS_MASTER + "EWS/" + DATASET_NAME + "/Rules/"

    var INPUT_DATASET_SUBJECT_OPERATOR_MAP = HDFS_MASTER + DATASET_NAME + "/Maps/SubjectOperatorMap/"
    var INPUT_DATASET_OPERATOR_SUBJECT_MAP = HDFS_MASTER + DATASET_NAME + "/Maps/OperatorSubjectMap/"
    var HORN_RULES = HDFS_MASTER + DATASET_NAME + "/Rules/"

    val hornRules = spark.read.json(HORN_RULES)
    val operatorSubjectMap = spark.read.json(INPUT_DATASET_OPERATOR_SUBJECT_MAP)
    val subjectOperatorMap = spark.read.json(INPUT_DATASET_SUBJECT_OPERATOR_MAP)

    val setDiff = udf((head: mutable.WrappedArray[String], body: mutable.WrappedArray[String]) => {
      body.diff(head)

    })
    def filterBody = udf((list: mutable.WrappedArray[String], body: mutable.WrappedArray[String]) => {
      list.intersect(body).size != 0
    })
    def removeEmpty = udf((array: Seq[String]) => !array.isEmpty)

    val operatorSupport = 0.03 * operatorSubjectMap.count

    val rulesWithFactsDF = hornRules
      .join(
        operatorSubjectMap,
        hornRules.col("body") === operatorSubjectMap.col("operator"))
      .withColumnRenamed("facts", "bodySet")
      .select("antecedent", "consequent", "bodySet")
      .join(hornRules
        .join(
          operatorSubjectMap,
          hornRules.col("head") === operatorSubjectMap.col("operator"))
        .withColumnRenamed("facts", "headSet")
        .select("antecedent", "consequent", "headSet"), Seq("antecedent", "consequent")) // Fact List

      .withColumn("setDiff", setDiff(col("headSet"), col("bodySet"))) // Difference in facts between body and head
      .filter(removeEmpty(col("setDiff"))) // Not consider rules which don't have this difference
      .withColumn("subject", explode(col("setDiff")))
      .join(subjectOperatorMap, "subject")

      .filter(filterBody(col("operators"), col("antecedent"))) // Get operators corresponding to the
      .withColumn("operator", explode(col("operators")))
      .drop("operators")
      .drop("subject")
      .drop("setdiff")
    val operatorSupportDF = rulesWithFactsDF.groupBy("antecedent", "consequent", "operator") // get operator support
      .agg(count("operator").as("support"))
      .filter(col("support") >= operatorSupport)
    val EWSWithFactsDF = rulesWithFactsDF
      .join(operatorSupportDF, Seq("antecedent", "consequent", "operator"))
      .join(operatorSubjectMap, "operator")
      .withColumnRenamed("facts", "operatorSet")

    val finalRules = EWSWithFactsDF.select(col("antecedent"), col("operator").as("negation"),
      col("consequent"), col("confidence"))

    println("Number of Exceptions " + finalRules.count())

    EWSWithFactsDF.write.mode(SaveMode.Overwrite).json(EWS_FACTS_WITH_RULES)

    finalRules.distinct.write.mode(SaveMode.Overwrite).json(EWS_RULES)

    spark.stop
  }

}
