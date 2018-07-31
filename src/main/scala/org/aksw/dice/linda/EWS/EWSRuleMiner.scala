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
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(INPUT_DATASET)

    RDF2TransactionMap.readFromDF(triplesDF)
    val subjectOperatorMap = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorMapSchema))
    val operatorSubjectMap = subjectOperatorMap
      .withColumn("operator", explode(col("operators"))).drop("operators")
      .groupBy(col("operator"))
      .agg(collect_list(col("subject")).as("subjects"))
    val removeEmpty = udf((array: Seq[String]) => !array.isEmpty)
    this.newFacts = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], resultSchema)
    fpgrowth.setItemsCol("items").setMinSupport(0.3).setMinConfidence(0.3)
    val model = fpgrowth.fit(subjectOperatorMap.select(col("operators").as("items")))

    val hornRules = model.associationRules
      .filter(removeEmpty(col("consequent")))
      .withColumn("body", explode(col("antecedent")))
      .withColumn("head", explode(col("consequent")))
    val setDiff = udf((head: mutable.WrappedArray[String], body: mutable.WrappedArray[String]) => {
      body.diff(head)

    })
    def filterBody = udf((list: mutable.WrappedArray[String], body: mutable.WrappedArray[String]) => {
      list.intersect(body).size != 0
    })

    val rulesWithFacts = hornRules
      .join(operatorSubjectMap, hornRules("body") === operatorSubjectMap("operator"))
      .withColumnRenamed("subjects", "bodySet")
      .select("antecedent", "consequent", "bodySet")
      .join(hornRules
        .join(operatorSubjectMap, hornRules("head") === operatorSubjectMap("operator"))
        .withColumnRenamed("subjects", "headSet")
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

    val operatorSupport = rulesWithFacts.groupBy("antecedent", "consequent", "operator") // get operator support
      .agg(count("operator").as("support"))
      .filter(col("support") >= 0.2 * subjectOperatorMap.count())
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
    val EWSWithFacts = rulesWithFacts
      .join(operatorSupport, Seq("antecedent", "consequent", "operator"))
      .join(operatorSubjectMap, "operator")
      .withColumnRenamed("subjects", "operatorSet")
      .withColumn("RuleSupport", getRuleSupport(col("headSet"), col("bodySet"), col("operatorSet")))
      .withColumn("BodySupport", getBodySupport(col("bodySet"), col("operatorSet")))
      .withColumn("confidence", col("RuleSupport").divide(col("BodySupport")))
      .filter(col("confidence") >= 0.2)
      .withColumn("newFacts", getFacts(col("bodySet"), col("operatorSet")))

    val facts = EWSWithFacts
      .select(col("consequent"), col("newFacts"), col("confidence"))
      .withColumn("s", explode(col("newFacts")))
      .withColumn("po", explode(col("consequent")))
      .withColumn("pred", cleanString(col("consequent")))
      .withColumn("_tmp", split(col("pred"), "\\,"))
      .select(
        col("confidence"),
        concat(lit("<"), col("s"), lit(">")),
        col("_tmp").getItem(0).as("p"),
        col("_tmp").getItem(1).as("o"))

    EWSWithFacts.select(col("antecedent"), col("operator").as("negation"),
      col("consequent"), col("confidence"))
      .write.mode(SaveMode.Overwrite).json(EWS_RULES_JSON)

    facts.distinct.write.mode(SaveMode.Overwrite)
      .option("header", "false")
      .option("delimiter", "\t").csv(FACTS_KB_EWS)

    spark.stop
  }

}