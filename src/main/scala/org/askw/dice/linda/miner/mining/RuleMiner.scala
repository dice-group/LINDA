package org.askw.dice.linda.miner.mining

import collection.mutable.{ HashMap }
import org.apache.spark.sql.{ SparkSession, _ }
import org.apache.spark.sql.expressions.Window;
import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._

import org.apache.spark.ml.fpm.FPGrowth
import scala.collection.mutable
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

import org.aksw.dice.linda.miner.datastructure.UnaryPredicate
import scala.collection.mutable.ListBuffer
import org.apache.spark.broadcast.Broadcast
object RuleMiner {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)
  val input = "Data/rdf.nt"
  var rules: DataFrame = _
  var subjectOperatorMap: DataFrame = _
  var transactionsDF: DataFrame = _
  var operator2Id: DataFrame = _
  val resourceIdSchema = List(StructField("resource", StringType, true))
  val subjectOperatorSchema = List(StructField("subject", StringType, true), StructField("operators", ArrayType(StringType, true), true))

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("LINDA (Miner)")
      .getOrCreate()
    val context = spark.sparkContext

    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(input)
    RDF2TransactionMap.readFromDF(triplesDF)
    this.subjectOperatorMap = spark.createDataFrame(RDF2TransactionMap.subject2Operator.map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorSchema))
    context.broadcast(this.subjectOperatorMap)

    // Rule Mining
    var transactionsRDD = RDF2TransactionMap.subject2Operator.map(r => r._2.map(a => a.toString()))
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.02).setMinConfidence(0.5)
    import spark.implicits._
    this.transactionsDF = transactionsRDD.toDF("items")
    val model = fpgrowth.fit(transactionsDF)
    var frequentOperator2Id = model.freqItemsets.drop("freq").map(r => r.getAs[Seq[String]]("items")).flatMap(a => a).withColumn("id", row_number().over(Window.orderBy("value")))
    var originalRules = model.associationRules

    calculateEWSUsingSetOperations(originalRules.first())
    //rules.foreach(r => calculateEWS(r))

    spark.stop
  }

  def calculateEWSUsingSetOperations(rule: Row) {
    val head = rule.getSeq(1)
    val body = rule.getSeq(0)

    def containsBody = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => body.contains(a))
    })
    def containshead = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => head.contains(a))
    })

    val bodyFacts = subjectOperatorMap.select(col("subject")).where(containsBody(col("operators"))).distinct()
    val headfacts = subjectOperatorMap.select(col("subject")).where(containshead(col("operators"))).distinct()

    val NS = headfacts.intersect(bodyFacts)
    val ABS = bodyFacts.except(headfacts)

    val difference = ABS.except(NS).rdd.map(r => r.getString(0)).collect()

    val EWS = subjectOperatorMap.select(col("operators").as("EWS")).where(col("subject").isin(difference: _*)).distinct
 
    //TODO add a min Confidence.
    //EWS.foreach(r => checkConfidence(r.getSeq(0), NS, bodyFacts))
   // checkConfidence(EWS.first().getSeq(0), NS, bodyFacts)

  }
  def checkConfidence(ews: Seq[Nothing], NS: Dataset[Row], bodyFacts: Dataset[Row]) = {
    def containsEWS = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => ews.contains(a))
    })
    lazy val EWSFacts = subjectOperatorMap.select(col("subject")).where(containsEWS(col("operators")))
    lazy val conf = EWSFacts.intersect(NS).count().toFloat / EWSFacts.intersect(bodyFacts).count().toFloat
    println(conf)
  }
  def calculateEWSUsingLearning(rule: Row) {
    val head = rule.getSeq(1)
    val body = rule.getSeq(0)
    val freqPatterns = new FPGrowth().setItemsCol("pattern").setMinSupport(0.02).setMinConfidence(0.5)
    def containsBodyandNotHead = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => (body.contains(a) && (!head.contains(a))))
    })
    def filterBody = udf((list: mutable.WrappedArray[String]) => {
      list.filter(!body.contains(_))
    })
    def containsBody = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => body.contains(a))
    })
    def containshead = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => head.contains(a))
    })
    val bodyFacts = subjectOperatorMap.select(col("subject")).where(containsBody(col("operators"))).distinct()
    val headfacts = subjectOperatorMap.select(col("subject")).where(containshead(col("operators"))).distinct()

    val negativeTransactions = transactionsDF.select(col("items")).where(containsBodyandNotHead(col("items"))).withColumn("pattern", filterBody(col("items"))).drop("items")
    val patternMiner = freqPatterns.fit(negativeTransactions)
    val EWS = patternMiner.freqItemsets
    EWS.show(false)
  }

}