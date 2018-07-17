package org.aksw.dice.linda.EWS

import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import org.aksw.dice.linda.Utils.LINDAProperties._
import org.apache.spark.sql.expressions.Window;
import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import org.apache.spark.ml.fpm.FPGrowth
import scala.collection.mutable
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.aksw.dice.linda.Utils.RDF2TransactionMap
import org.aksw.dice.linda.Utils.LINDAProperties._

object EWSRuleMiner {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  var rules: DataFrame = _
  var subjectOperatorMap: DataFrame = _
  var operatorSubjectMap: DataFrame = _
  var transactionsDF: DataFrame = _
  var operator2Id: DataFrame = _
  val resourceIdSchema = List(StructField("resource", StringType, true))
  val subjectOperatorMapSchema = List(StructField("subject", StringType, true), StructField("operators", ArrayType(StringType, true), true))
  val operatorSubjectSchema = List(StructField("subject", StringType, true), StructField("operators", ArrayType(StringType, true), true))

  val fpgrowth = new FPGrowth()

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .config(WAREHOUSE, DIRECTORY)
      .appName(APP_EWS_MINER)
      .getOrCreate()
    val context = spark.sparkContext
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(INPUT_DATASET)

    RDF2TransactionMap.readFromDF(triplesDF)

    this.subjectOperatorMap = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorMapSchema))
      .withColumn("factConf", lit(1.0))

    this.operatorSubjectMap = this.subjectOperatorMap
      .withColumn("operator", explode(col("operators"))).drop("operators")
      .drop("factConf").groupBy(col("operator"))
      .agg(collect_list(col("subject")).as("subjects"))
    fpgrowth.setItemsCol("items").setMinSupport(0.02).setMinConfidence(0.5)
    val model = fpgrowth.fit(this.subjectOperatorMap.select(col("operators").as("items")))

    val newRules = model.associationRules.limit(5).withColumn(
      "EWS",
      calculateEWSUsingLearning(struct(col("antecedent"), col("consequent"))))
      .withColumn("negation", explode(col("EWS"))).drop("EWS")

    //this.subjectOperatorMap.write.mode(SaveMode.Overwrite).parquet(INPUT_DATASET_SUBJECT_OPERATOR_MAP)
    // newRules.write.mode 	(SaveMode.Overwrite).format("json").save(EWS_RULES_JSON)
    newRules.show(false)
    //write.mode(SaveMode.Overwrite).parquet(EWS_RULES)*/
    spark.stop
  }

  def calculateEWS(rule: Row) {

    // .rdd.map(r => r.getString(0)).collect().toList
  }

  def calculateEWSUsingLearning = udf((rule: Row) => {
    val head = rule.getSeq(1)
    val body = rule.getSeq(0)
    fpgrowth.setMinConfidence(0.0).setMinSupport(0.01).setItemsCol("patterns")

    def filterBody = udf((list: mutable.WrappedArray[String]) => {
      list.filter(!body.contains(_))
    })

    val bodyFacts = operatorSubjectMap.select(col("subjects"))
      .where(col("operator").isin(body: _*)).withColumn("subject", explode(col("subjects"))).drop("subjects")
    val headFacts = operatorSubjectMap.select(col("subjects"))
      .where(col("operator").isin(head: _*)).withColumn("subject", explode(col("subjects"))).drop("subjects")

    val negativeTransactions = bodyFacts.except(headFacts).join(subjectOperatorMap, "subject")
      .withColumn("patterns", filterBody(col("operators")))
      .select("patterns")

    val EWS = fpgrowth.fit(negativeTransactions).freqItemsets
      .withColumn("operator", explode(col("items")))
      .drop(col("items"))
      .drop(col("freq"))

    val ewsElements = subjectOperatorMap.join(headFacts, "subject").union(subjectOperatorMap.join(bodyFacts, "subject"))
      .withColumn("operator", explode(col("operators")))
      .drop("operators")
      .drop("factConf")
      .join(operatorSubjectMap.join(EWS, "operator")
        .drop("EWS")
        .withColumn("subject2", explode(col("subjects")))
        .drop("subjects"), "operator")
    val ewsTransaction = ewsElements.select("operator", "subject")
      .union(ewsElements.select("operator", "subject2"))
      .groupBy(col("operator")).agg(count("*")
        .as("numerator"))
    val bodyElements = subjectOperatorMap.join(bodyFacts, "subject")
      .withColumn("operator", explode(col("operators")))
      .drop("operators")
      .drop("factConf")
      .join(operatorSubjectMap.join(EWS, "operator")
        .drop("EWS")
        .withColumn("subject2", explode(col("subjects")))
        .drop("subjects"), "operator")
    val bodyTransaction = bodyElements.select("operator", "subject").union(bodyElements.select("operator", "subject2"))
      .groupBy(col("operator")).agg(count("*")
        .as("denominator"))
    ewsTransaction.join(bodyTransaction, "operator")
      .withColumn("confidence", col("numerator")
        .divide(col("denominator"))).filter("confidence >= 0.3")
      .drop(col("numerator"))
      .drop(col("denominator"))
      .rdd.map(r => r.getString(0)).collect().toList

  })

  def calculateEWSUsingSetOperations = udf((rule: Row) => {
    val head = rule.getSeq(1)
    val body = rule.getSeq(0)

    val bodyFacts = operatorSubjectMap.select(col("subjects"))
      .where(col("operator").isin(body: _*)).withColumn("subject", explode(col("subjects"))).drop("subjects")
    val headFacts = operatorSubjectMap.select(col("subjects"))
      .where(col("operator").isin(head: _*)).withColumn("subject", explode(col("subjects"))).drop("subjects")
    val differenceBodyandHead = bodyFacts.except(headFacts)

    val EWS = differenceBodyandHead.join(operatorSubjectMap.withColumn("subject", explode(col("subjects"))), "subject")
      .drop("factConf")
      .drop("subjects")
      .select(col("operator"))

    val ewsElements = subjectOperatorMap.join(headFacts, "subject").union(subjectOperatorMap.join(bodyFacts, "subject"))
      .withColumn("operator", explode(col("operators")))
      .drop("operators")
      .drop("factConf")
      .join(operatorSubjectMap.join(EWS, "operator")
        .drop("EWS")
        .withColumn("subject2", explode(col("subjects")))
        .drop("subjects"), "operator")
    val ewsTransaction = ewsElements.select("operator", "subject")
      .union(ewsElements.select("operator", "subject2"))
      .groupBy(col("operator")).agg(count("*")
        .as("numerator"))

    val bodyElements = subjectOperatorMap.join(bodyFacts, "subject")
      .withColumn("operator", explode(col("operators")))
      .drop("operators")
      .drop("factConf")
      .join(operatorSubjectMap.join(EWS, "operator")
        .drop("EWS")
        .withColumn("subject2", explode(col("subjects")))
        .drop("subjects"), "operator")
    val bodyTransaction = bodyElements.select("operator", "subject").union(bodyElements.select("operator", "subject2"))
      .groupBy(col("operator")).agg(count("*")
        .as("denominator"))
    ewsTransaction.join(bodyTransaction, "operator")
      .withColumn("confidence", col("numerator")
        .divide(col("denominator"))).filter("confidence >= 0.3")
      .drop(col("numerator"))
      .drop(col("denominator"))
      .rdd.map(r => r.getString(0)).collect().toList
  })
}