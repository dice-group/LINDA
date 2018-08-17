package org.aksw.dice.linda.EWS
import org.aksw.dice.linda.Utils.LINDAProperties._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import scala.collection.mutable

object FactGenerator {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .appName(APP_EWS_FACTS)
      .getOrCreate()

    if (args.length == 0) {
      println("No Parameters provided")
      spark.stop()
    }
    def getFacts = udf((body: mutable.WrappedArray[String], neg: mutable.WrappedArray[String]) => {
      body.intersect(neg)
    })
    def getRuleSupport = udf((head: mutable.WrappedArray[String], body: mutable.WrappedArray[String], neg: mutable.WrappedArray[String]) => {
      head.intersect(body).intersect(neg).size
    })
    def getBodySupport = udf((body: mutable.WrappedArray[String], neg: mutable.WrappedArray[String]) => {
      body.intersect(neg).size
    })
    var DATASET_NAME = args(0)
    var HDFS_MASTER = args(1)
    var EWS_FACTS_WITH_RULES = HDFS_MASTER + "EWS/" + DATASET_NAME + "/EWSfactswithRules/"
    var FACTS_KB_EWS = HDFS_MASTER + "EWS/" + DATASET_NAME + "/Facts/"
    def cleanString = udf((body: mutable.WrappedArray[String]) => {
      body.mkString("").replace("[", "").replace("]", "")
    })

    val EWSWithFactsDF = spark.read.json(EWS_FACTS_WITH_RULES)
      .withColumn("RuleSupport", getRuleSupport(
        col("headSet"),
        col("bodySet"), col("operatorSet")))
      .withColumn("BodySupport", getBodySupport(col("bodySet"), col("operatorSet")))
      .withColumn("confidence", col("RuleSupport").divide(col("BodySupport")))
      .filter(col("confidence") >= 0.01)
      
      
    val facts = EWSWithFactsDF
      .withColumn("newFacts", getFacts(col("bodySet"), col("operatorSet")))
      .select(col("consequent"), col("newFacts"), col("confidence"))
      .withColumn("s", explode(col("newFacts")))
      .withColumn("po", explode(col("consequent")))
      .withColumn("pred", cleanString(col("consequent")))
      .withColumn("_tmp", split(col("pred"), "\\::"))
      .withColumn("triple", concat(
        col("s"), lit(" "),
        col("_tmp").getItem(0).as("p"), lit(" "),
        col("_tmp").getItem(1).as("o")))
      .select(col("confidence"), col("triple"))

    println("Number of Facts " + facts.count())
    facts.write.mode(SaveMode.Overwrite)
      .option("header", "false")
      .option("delimiter", "\t").csv(FACTS_KB_EWS)

    spark.stop
  }
}