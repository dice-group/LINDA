package org.askw.dice.linda.miner.PatternRuleMining

import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import org.apache.spark.sql.expressions.Window;
import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import org.apache.spark.ml.fpm.FPGrowth
import scala.collection.mutable
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.aksw.dice.linda.miner.datastructure.RDF2TransactionMap

object RuleMiner {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)
  val input = "Data/rdf.nt"
  val Name = "rdf"
  var rules: DataFrame = _
  var subjectOperatorMap: DataFrame = _
  var transactionsDF: DataFrame = _
  var operator2Id: DataFrame = _
  val resourceIdSchema = List(StructField("resource", StringType, true))
  val subjectOperatorSchema = List(StructField("subject", StringType, true), StructField("operators", ArrayType(StringType, true), true))
  val fpgrowth = new FPGrowth()
  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("LINDA  (Original Miner)")
      .getOrCreate()
    val context = spark.sparkContext
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(input)
    RDF2TransactionMap.readFromDF(triplesDF)
    this.subjectOperatorMap = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorSchema))
      .withColumn("factConf", lit(1.0))

    var subjects = RDF2TransactionMap.subject2Operator.map(r => Row(r._1))
    var subject2Id = spark.createDataFrame(subjects, StructType(resourceIdSchema))
      .withColumn("id", row_number().over(Window.orderBy("resource")))
    var transactionsRDD = RDF2TransactionMap.subject2Operator.map(r => r._2.map(a => a.toString()))
    import spark.implicits._
    this.transactionsDF = transactionsRDD.toDF("items")
    fpgrowth.setItemsCol("items").setMinSupport(0.02).setMinConfidence(0.5)
    val model = fpgrowth.fit(transactionsDF)
    val newRules = model.associationRules.limit(5).withColumn(
      "EWS",
      calculateEWSUsingSetOperations(struct(col("antecedent"), col("consequent"))))
      .withColumn("negation", explode(col("EWS"))).drop("EWS")
    // newRules.write.format("json").save("Data/OriginalAlgorithm/NewRules/" + Name+"/json")
   //newRules.write.format("parquet").save("Data/OriginalAlgorithm/NewRules/" + Name + "/parquet")
    this.subjectOperatorMap.write.parquet("Data/OriginalAlgorithm/NewRules/" + Name + "/oriignalKB")
    spark.stop
  }
  def calculateEWSUsingLearning = udf((rule: Row) => {
    val head = rule.getSeq(1)
    val body = rule.getSeq(0)
    fpgrowth.setMinConfidence(0.0).setMinSupport(0.01).setItemsCol("patterns")
    def containsBody = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => body.contains(a))
    })
    def containshead = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => head.contains(a))
    })
    def filterBody = udf((list: mutable.WrappedArray[String]) => {
      list.filter(!body.contains(_))
    })
    def containsEWS = udf((list: mutable.WrappedArray[String], ele: String) => {
      list.exists(a => a.contains(ele))
    })
    val negativeTransactions = transactionsDF.select(col("items"))
      .where(containsBody(col("items"))).where(!containshead(col("items")))
      .withColumn("patterns", filterBody(col("items"))).drop("items")
    val EWS = fpgrowth.fit(negativeTransactions).freqItemsets
      .withColumn("EWS", explode(col("items")))
      .drop(col("items"))
      .drop(col("freq"))
    val ewsTransaction = transactionsDF.select(col("items"))
      .where(containsBody(col("items")) && containshead(col("items")))
      .join(EWS, containsEWS(col("items"), col("EWS")))
      .groupBy(col("EWS")).agg(count("*").as("numerator"))
    val bodyTransaction = transactionsDF.select(col("items"))
      .where(containsBody(col("items"))).join(EWS, containsEWS(col("items"), col("EWS")))
      .groupBy(col("EWS")).agg(count("*").as("denominator"))
    ewsTransaction.join(bodyTransaction, "EWS")
      .withColumn("confidence", col("numerator").divide(col("denominator")))
      .filter("confidence >= 0.3")
      .drop(col("numerator"))
      .drop(col("denominator")).rdd.map(r => r.getString(0)).collect().toList

  })

  def calculateEWSUsingSetOperations = udf((rule: Row) => {
    val head = rule.getSeq(1)
    val body = rule.getSeq(0)
    def containsBody = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => body.contains(a))
    })
    def containshead = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => head.contains(a))
    })
    def containsEWS = udf((list: mutable.WrappedArray[String], ele: String) => {
      list.exists(a => a.contains(ele))
    })
    val bodyfacts = subjectOperatorMap.select(col("subject"))
      .where(containsBody(col("operators"))).distinct()
    val headfacts = subjectOperatorMap.select(col("subject"))
      .where(containshead(col("operators"))).distinct()

    val differenceABSandNS = bodyfacts.except(headfacts)
      .except(headfacts.intersect(bodyfacts))
      .rdd.map(r => r.getString(0)).collect()
    val EWS = subjectOperatorMap.select(col("operators"))
      .where(col("subject").isin(differenceABSandNS: _*)).distinct()
      .withColumn("EWS", explode(col("operators")))
      .drop(col("operators"))
    val ewsTransaction = transactionsDF.select(col("items"))
      .where(containsBody(col("items")) && containshead(col("items"))).join(EWS, containsEWS(col("items"), col("EWS")))
      .groupBy(col("EWS")).agg(count("*").as("numerator"))
    val bodyTransaction = transactionsDF.select(col("items"))
      .where(containsBody(col("items")))
      .join(EWS, containsEWS(col("items"), col("EWS")))
      .groupBy(col("EWS")).agg(count("*")
        .as("denominator"))
    ewsTransaction.join(bodyTransaction, "EWS")
      .withColumn("confidence", col("numerator")
        .divide(col("denominator"))).filter("confidence >= 0.3")
      .drop(col("numerator"))
      .drop(col("denominator"))
      .rdd.map(r => r.getString(0)).collect().toList
  })
}