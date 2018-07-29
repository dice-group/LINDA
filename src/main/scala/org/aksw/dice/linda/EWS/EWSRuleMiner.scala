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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.plans.logical.With

object EWSRuleMiner {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  val resultSchema = StructType(
    StructField("conf", IntegerType, true) ::
      StructField("s", StringType, true) ::
      StructField("p", StringType, true) ::
      StructField("o", StringType, true) :: Nil)
  var newFacts: DataFrame = _
  val resourceIdSchema = List(StructField("resource", StringType, true))
  val subjectOperatorMapSchema = List(
    StructField("subject", StringType, true),
    StructField("operators", ArrayType(StringType, true), true))
  val operatorSubjectSchema = List(
    StructField("subject", StringType, true),
    StructField("operators", ArrayType(StringType, true), true))

  val fpgrowth = new FPGrowth()

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .appName(APP_EWS_MINER)
      .getOrCreate()
    val context = spark.sparkContext
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)("/Users/Kunal/workspaceThesis/LINDA/Data/rdf.nt")

    RDF2TransactionMap.readFromDF(triplesDF)
    val subjectOperatorMap = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorMapSchema))
    val operatorSubjectMap = subjectOperatorMap
      .withColumn("operator", explode(col("operators"))).drop("operators")
      .groupBy(col("operator"))
      .agg(collect_list(col("subject")).as("subjects"))
    val removeEmpty = udf((array: Seq[String]) => !array.isEmpty)
    this.newFacts = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], resultSchema)
    fpgrowth.setItemsCol("items").setMinSupport(0.01).setMinConfidence(0.1)
    val model = fpgrowth.fit(subjectOperatorMap.select(col("operators").as("items")))

    val newRules = model.associationRules.filter(removeEmpty(col("consequent"))).withColumn("body", explode(col("antecedent")))
      .withColumn("head", explode(col("consequent")))
    val setDiff = udf((head: mutable.WrappedArray[String], body: mutable.WrappedArray[String]) => {
      body.diff(head)

    })
    def filterBody = udf((list: mutable.WrappedArray[String], body: mutable.WrappedArray[String]) => {
      list.intersect(body).size != 0
    })
    val rulesWithFacts = newRules
      .join(operatorSubjectMap, newRules("body") === operatorSubjectMap("operator"))
      .withColumnRenamed("subjects", "bodySet")
      .select("antecedent", "consequent", "bodySet")
      .join(newRules
        .join(operatorSubjectMap, newRules("head") === operatorSubjectMap("operator"))
        .withColumnRenamed("subjects", "headSet")
        .select("antecedent", "consequent", "headSet"), Seq("antecedent", "consequent"))
      .withColumn("setDiff", setDiff(col("headSet"), col("bodySet")))
      .filter(removeEmpty(col("setDiff")))
      .withColumn("subject", explode(col("setDiff")))
      .join(subjectOperatorMap, "subject")
      .filter(filterBody(col("operators"), col("antecedent")))
      .drop("headSet")
      .drop("subject")
      .drop("setdiff")
      .drop("bodySet")

    rulesWithFacts.show(false)
    /*
.withColumn("operator", explode(col("patterns")))
      .groupBy("antecedent", "consequent")
      .agg(count("operator").as("cnt"))
   .withColumn(
      "EWS", calculateEWSUsingLearning(struct(col("antecedent"), col("consequent"))))
      .withColumn("negation", explode(col("EWS"))).drop("EWS")
    newRules.show(false)
    newRules.foreach(r => generateFacts(r))
    this.newFacts.select(col("conf"), concat(lit("<"), col("s"), lit(">"), lit(" "), lit("<"), col("p"), lit(">"), lit(" "), lit("<"), col("o"), lit(">")))
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header", "false")
      .option("delimiter", "\t").csv(FACTS_KB_EWS)
    operatorSubjectMap.write.mode(SaveMode.Overwrite).parquet(INPUT_DATASET_OPERATOR_SUBJECT_MAP)
    newRules.withColumnRenamed("antecedent", "body")
      .withColumnRenamed("negation", "negative")
      .withColumnRenamed("consequent", "head")
      .write.mode(SaveMode.Overwrite).json(EWS_RULES_JSON)*/
    spark.stop
  }
  /*
  val calculateEWSUsingLearning = udf((rule: Row) => {

    fpgrowth.setMinConfidence(0.0).setMinSupport(0.01).setItemsCol("patterns")

    val EWS = fpgrowth.fit(negativeTransactions).freqItemsets
      .withColumn("operator", explode(col("items")))
      .drop(col("items"))
      .drop(col("freq"))

    val ewsElements = subjectOperatorMap.join(headFacts, "subject")
      .union(subjectOperatorMap.join(bodyFacts, "subject"))
      .withColumn("operator", explode(col("operators")))
      .drop("operators")
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
      .join(operatorSubjectMap.join(EWS, "operator")
        .drop("EWS")
        .withColumn("subject2", explode(col("subjects")))
        .drop("subjects"), "operator")
    val bodyTransaction = bodyElements.select("operator", "subject").union(bodyElements.select("operator", "subject2"))
      .groupBy(col("operator")).agg(count("*")
        .as("denominator"))
    ewsTransaction.join(bodyTransaction, "operator")
      .withColumn("confidence", col("numerator")
        .divide(col("denominator"))).filter("confidence >= 0.2")
      .drop(col("numerator"))
      .drop(col("denominator"))
      .rdd.map(r => r.getString(0)).collect().toList
  })
  val calculateEWSUsingSetOperations = udf((rule: Row) => {
    val head = rule.getSeq(1)
    val body = rule.getSeq(0)
    val bodyFacts = operatorSubjectMap.select(col("subjects"))
      .where(col("operator").isin(body: _*)).withColumn("subject", explode(col("subjects"))).drop("subjects")
    val headFacts = operatorSubjectMap.select(col("subjects"))
      .where(col("operator").isin(head: _*)).withColumn("subject", explode(col("subjects"))).drop("subjects")
    val differenceBodyandHead = bodyFacts.except(headFacts)

    val EWS = differenceBodyandHead.join(operatorSubjectMap.withColumn("subject", explode(col("subjects"))), "subject")

      .drop("subjects")
      .select(col("operator"))

    val ewsElements = subjectOperatorMap.join(headFacts, "subject").union(subjectOperatorMap
      .join(bodyFacts, "subject"))
      .withColumn("operator", explode(col("operators")))
      .drop("operators")

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
  def generateFacts(rule: Row): Unit = {
    val body = rule.getSeq(0)
    val head = rule.getSeq[String](1)(0)
    val ele = head.replaceAll("<", "").replaceAll(">", "").split(",")
    val confidence = rule.getDouble(2)
    val negation = rule.getString(3)

    this.newFacts = this.newFacts.union(operatorSubjectMap.select(col("subjects"))
      .where(col("operator").isin(body: _*))
      .withColumn("subject", explode(col("subjects")))
      .drop("subjects")
      .intersect(subjectOperatorMap.select(col("subject"))
        .where(array_contains(col("operators"), negation)))
      .withColumn("conf", lit(confidence))
      .withColumn("p", lit(ele(0)))
      .withColumn("o", lit(ele(1)))
      .withColumnRenamed("subject", "s")
      .select("conf", "s", "p", "o"))

  }*/

}