package org.aksw.dice.linda.EWS

import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import org.aksw.dice.linda.Utils.LINDAProperties._
import org.apache.spark.sql.expressions.Window;
import org.slf4j.LoggerFactory
import org.apache.spark.ml.fpm.FPGrowth
import scala.collection.mutable
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import org.aksw.dice.linda.Utils.LINDAProperties._
import org.aksw.dice.linda.Utils.TripleUtils
import org.aksw.dice.linda.Utils.LINDAProperties

object EWSRuleMiner {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .appName(APP_EWS_MINER)
      .getOrCreate()
    val fpgrowth = new FPGrowth()

    val context = spark.sparkContext

    if (args.length == 0) {
      println("No Parameters provided")
      spark.stop()
    }

    LINDAProperties.DATASET_NAME = args(0)
    LINDAProperties.INPUT_DATASET = args(1)
    LINDAProperties.HDFS_MASTER = args(2)

    def getRuleSupport = udf((head: mutable.WrappedArray[String], body: mutable.WrappedArray[String], neg: mutable.WrappedArray[String]) => {
      head.intersect(body).intersect(neg).size
    })
    def getBodySupport = udf((body: mutable.WrappedArray[String], neg: mutable.WrappedArray[String]) => {
      body.intersect(neg).size
    })

    def getFacts = udf((body: mutable.WrappedArray[String], neg: mutable.WrappedArray[String]) => {
      body.intersect(neg)
    })
    def cleanString = udf((body: mutable.WrappedArray[String]) => {
      body.mkString("").replace("[", "").replace("]", "")
    })

    val setDiff = udf((head: mutable.WrappedArray[String], body: mutable.WrappedArray[String]) => {
      body.diff(head)

    })
    def filterBody = udf((list: mutable.WrappedArray[String], body: mutable.WrappedArray[String]) => {
      list.intersect(body).size != 0
    })
    // "/Users/Kunal/Desktop/imdb.nt" (
    val triplesDF =
      spark.createDataFrame(spark.sparkContext.textFile("/Users/Kunal/workspaceThesis/LINDA/Data/rdf.nt")
        .filter(!_.startsWith("#")).map(data => TripleUtils.parsTriples(data)))
        .withColumn(
          "unaryOperator",
          concat(col("predicate"), lit(",<"), col("object")))

    val numberofTriples = triplesDF.count()
    val operatorSupport = 0.03 * numberofTriples

    println(DATASET_NAME + "::::  Number of Triples :  " + numberofTriples)

    val subjectOperatorMap = triplesDF.groupBy(col("subject"))
      .agg(collect_set(col("unaryOperator")).as("operators"))

    val operatorSubjectMap = triplesDF.groupBy(col("unaryOperator"))
      .agg(collect_set(col("subject")).as("facts"))
      .withColumnRenamed("unaryOperator", "operator")

    fpgrowth.setItemsCol("items")
      .setMinSupport(0.002)
      .setMinConfidence(0.01)

    val model = fpgrowth.fit(subjectOperatorMap.select(col("operators").as("items")))

    val originalRules = model.associationRules
    println("Number of ORiginal Rules" + originalRules.count())
    val removeEmpty = udf((array: Seq[String]) => !array.isEmpty)

    val hornRules = originalRules
      .filter(removeEmpty(col("consequent")))
      .withColumn("body", explode(col("antecedent")))
      .withColumn("head", explode(col("consequent")))

    val rulesWithFactsDF = hornRules
      .join(operatorSubjectMap, hornRules("body") === operatorSubjectMap("operator"))
      .withColumnRenamed("facts", "bodySet")
      .select("antecedent", "consequent", "bodySet")
      .join(hornRules
        .join(operatorSubjectMap, hornRules("head") === operatorSubjectMap("operator"))
        .withColumnRenamed("facts", "headSet")
        .select("antecedent", "consequent", "headSet"), Seq("antecedent", "consequent"))
      // Fact List
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
      .withColumn("RuleSupport", getRuleSupport(
        col("headSet"),
        col("bodySet"), col("operatorSet")))
      .withColumn("BodySupport", getBodySupport(col("bodySet"), col("operatorSet")))
      .withColumn("confidence", col("RuleSupport").divide(col("BodySupport")))
      .filter(col("confidence") >= 0.01)
      .withColumn("newFacts", getFacts(col("bodySet"), col("operatorSet")))

    val facts = EWSWithFactsDF
      .select(col("consequent"), col("newFacts"), col("confidence"))
      .withColumn("s", explode(col("newFacts")))
      .withColumn("po", explode(col("consequent")))
      .withColumn("pred", cleanString(col("consequent")))
      .withColumn("_tmp", split(col("pred"), "\\,"))
      .withColumn("triple", concat(
        col("s"), lit(" "),
        col("_tmp").getItem(0).as("p"), lit(" "),
        col("_tmp").getItem(1).as("o")))
      .select(col("confidence"), col("triple"))

    val finalRules = EWSWithFactsDF.select(col("antecedent"), col("operator").as("negation"),
      col("consequent"), col("confidence"))

    println("Number of Exceptions" + finalRules.count())
    /*
    println("Number of Facts " + facts.count())
    finalRules.distinct.write.mode(SaveMode.Overwrite).json(EWS_RULES_JSON)
    facts.write.mode(SaveMode.Overwrite)
      .option("header", "false")
      .option("delimiter", "\t").csv(FACTS_KB_EWS)
*/
    spark.stop
  }

}
