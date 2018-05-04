package org.askw.dice.linda.miner.mining

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
import org.apache.spark.sql.Encoders
import org.aksw.dice.linda.miner.datastructure.UnaryPredicate
import org.datanucleus.query.expression.JoinExpression

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
    var subjects = RDF2TransactionMap.subject2Operator.map(r => Row(r._1))
    var subject2Id = spark.createDataFrame(subjects, StructType(resourceIdSchema)).withColumn("id", row_number().over(Window.orderBy("resource")))
    var transactionsRDD = RDF2TransactionMap.subject2Operator.map(r => r._2.map(a => a.toString()))
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.02).setMinConfidence(0.5)
    import spark.implicits._
    this.transactionsDF = transactionsRDD.toDF("items")
    val model = fpgrowth.fit(transactionsDF)
    var frequentOperator2Id = model.freqItemsets.drop("freq").map(r => r.getAs[Seq[String]]("items")).flatMap(a => a).withColumn("id", row_number().over(Window.orderBy("value")))
    var rules = model.associationRules

    //rules.foreach(r => calculateEWS(r))
    val b = calculateEWSUsingLearning(rules.first())

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
    val bodyfacts = subjectOperatorMap.select(col("subject")).where(containsBody(col("operators"))).distinct()
    val headfacts = subjectOperatorMap.select(col("subject")).where(containshead(col("operators"))).distinct()

    val NS = headfacts.intersect(bodyfacts)
    val ABS = bodyfacts.except(headfacts)

    val difference = ABS.except(NS).rdd.map(r => r.getString(0)).collect()
    val EWS = subjectOperatorMap.select(col("operators")).where(col("subject").isin(difference: _*)).distinct().withColumn("EWS", explode(col("operators"))).drop(col("operators"))

    //TODO add a min support count.

  }
  def calculateEWSUsingLearning(rule: Row) {
    val head = rule.getSeq(1)
    val body = rule.getSeq(0)
    val freqPatterns = new FPGrowth().setItemsCol("patterns").setMinSupport(0.1)
    def containsBodyAndNotHead = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => (body.contains(a) && (!head.contains(a))))
    })
    def filterBody = udf((list: mutable.WrappedArray[String]) => {
      list.filter(!body.contains(_))
    })

    val negativeTransactions = transactionsDF.select(col("items")).where(containsBodyAndNotHead(col("items"))).withColumn("patterns", filterBody(col("items"))).drop("items")
    val patternMiner = freqPatterns.fit(negativeTransactions)
    val EWS = patternMiner.freqItemsets.withColumn("EWS", explode(col("items"))).drop(col("items")).drop(col("freq"))

    def containsBody = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => body.contains(a))
    })
    def containshead = udf((list: mutable.WrappedArray[String]) => {
      list.exists(a => head.contains(a))
    })
    def containsEWS = udf((list: mutable.WrappedArray[String], ele: String) => {
      list.exists(a => a.contains(ele))
    })

    val filteredTransaction = transactionsDF.select(col("items")).where(containsBody(col("items")) && containshead(col("items"))).join(EWS, containsEWS(col("items"), col("EWS")))
    filteredTransaction.show(false)
  }

}