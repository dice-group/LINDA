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
import org.apache.spark.storage.StorageLevel

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
    val setDiff = udf((head: mutable.WrappedArray[String], body: mutable.WrappedArray[String]) => {
      body.diff(head)

    })
    def filterBodyFacts = udf((list: mutable.WrappedArray[String], body: mutable.WrappedArray[String]) => {
      list.intersect(body).size != 0
    })
    def filterBody = udf((list: String, body: mutable.WrappedArray[String]) => {
      !body.contains(list)
    })
    def filterHead = udf((list: String, head: mutable.WrappedArray[String]) => {
      !head.contains(list)
    })
    def getRuleSupport = udf((head: mutable.WrappedArray[String], body: mutable.WrappedArray[String], neg: mutable.WrappedArray[String]) => {
      head.intersect(body).intersect(neg).size
    })
    def getBodySupport = udf((body: mutable.WrappedArray[String], neg: mutable.WrappedArray[String]) => {
      body.intersect(neg).size
    })

    def removeEmpty = udf((array: Seq[String]) => !array.isEmpty)
    val hornRules = spark.read.json(HORN_RULES).cache()
    //.repartition(1)
    val operatorSubjectMap = spark.read.json(INPUT_DATASET_OPERATOR_SUBJECT_MAP).cache()

    val subjectOperatorMap = spark.read.json(INPUT_DATASET_SUBJECT_OPERATOR_MAP)

    val bodyFacts = hornRules
      .join(
        operatorSubjectMap,
        hornRules.col("body") === operatorSubjectMap.col("operator"))
      .withColumnRenamed("facts", "bodySet")
      .select("antecedent", "consequent", "bodySet")

    val headFacts = hornRules.join(
      operatorSubjectMap,
      hornRules.col("head") === operatorSubjectMap.col("operator"))
      .withColumnRenamed("facts", "headSet")
      .select("antecedent", "consequent", "headSet")

    val allFacts = bodyFacts.join(headFacts, Seq("antecedent", "consequent"))
      .withColumn("setDiff", setDiff(col("headSet"), col("bodySet"))) // Difference in facts between body and head
      .filter(removeEmpty(col("setDiff"))) // Not consider rules which don't have this difference
      .withColumn("subject", explode(col("setDiff")))

    val rulesWithFactsDF = allFacts.join(subjectOperatorMap, "subject")
      .withColumn("operator", explode(col("operators")))
      .drop("operators")
      .drop("subject")
      .drop("setdiff")
      .cache()
    //    .repartition(5)

    val ewsSupportWindown = Window.partitionBy("antecedent", "consequent", "operator")

    val EWSWithFactsDF = rulesWithFactsDF
      .join(operatorSubjectMap, "operator")
      .filter(filterBody(col("operator"), col("antecedent")))
      .filter(filterHead(col("operator"), col("consequent")))
      .withColumnRenamed("facts", "operatorSet")
      .withColumn("RuleSupport", getRuleSupport(
        col("headSet"),
        col("bodySet"), col("operatorSet")))
      .withColumn("BodySupport", getBodySupport(col("bodySet"), col("operatorSet")))
      .withColumn("confidence", col("RuleSupport").divide(col("BodySupport")))
      .filter(col("confidence") >= 0.1)
      .cache()

    val finalRules = EWSWithFactsDF.select(col("antecedent"), col("operator").as("negation"),
      col("consequent"))
    println("Number of Exceptions " + finalRules.count())
    // finalRules.show(false)

    finalRules.write.mode(SaveMode.Overwrite).json(EWS_RULES)
    // EWSWithFactsDF.coalesce(1).write.mode(SaveMode.Overwrite).json(EWS_FACTS_WITH_RULES)

    spark.stop
  }

}
