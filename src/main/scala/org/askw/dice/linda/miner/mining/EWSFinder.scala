package org.askw.dice.linda.miner.mining

import org.apache.spark.sql.{ Row, SparkSession, Encoder, _ }
import org.slf4j.LoggerFactory
import scala.collection.mutable
import net.sansa_stack.rdf.spark.io.rdf._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, MatrixEntry }
import org.aksw.dice.linda.miner.datastructure.UnaryPredicate
import org.apache.spark.sql.expressions.Window;
import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.aksw.dice.linda.miner.datastructure.UnaryPredicate

object EWSMining {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  var rules: DataFrame = _
  var subjectOperatorMap: DataFrame = _
  var operator2Id: DataFrame = _
  val spark = SparkSession.builder
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName("LINDA (EWS Finder)")
    .getOrCreate()

  def main(args: Array[String]) = {

    val input = "Data/rdf.nt"
    val context = spark.sparkContext
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(input)
    RDF2TransactionMap.readFromDF(triplesDF)
    val resourceIdSchema = List(StructField("resource", StringType, true))
    val subjectOperatorSchema = List(StructField("subject", StringType, true), StructField("operators", ArrayType(StringType, true), true))
    var subjetct2Operator = spark.createDataFrame(RDF2TransactionMap.subject2Operator.map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorSchema))
    var subjects = RDF2TransactionMap.subject2Operator.map(r => Row(r._1))
    var subject2Id = spark.createDataFrame(subjects, StructType(resourceIdSchema)).withColumn("id", row_number().over(Window.orderBy("resource")))
    var transactionsRDD = RDF2TransactionMap.subject2Operator.map(r => r._2.map(a => a.toString()))
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.02).setMinConfidence(0.5)
    import spark.implicits._
    val model = fpgrowth.fit(transactionsRDD.toDF("items"))
    var rules = model.associationRules.withColumn("antecedent", explode(col("antecedent")))
    spark.stop
  }

  def calculateEWS(rule: Row) {

    val head = rule.getString(0)
    println(head)
    val body = rule.getAs[mutable.WrappedArray[String]](1).toIterable
    var headFacts = subjectOperatorMap.select(subjectOperatorMap("subject")).filter(array_contains(subjectOperatorMap("operators"), head))
    var bodyFacts = subjectOperatorMap.select(subjectOperatorMap("subject")).filter(subjectOperatorMap("operators").isin(body))

    bodyFacts.show()
  }
}

/*  val rulesPath = "Data/rules/"
    val mapsPath = "Data/Maps/"

    val context = spark.sparkContext

    var a = spark.read.parquet(rulesPath)
    subjectOperatorMap = spark.read.parquet(mapsPath + "SubjectOperatorMap/*")
    rules = a.withColumn("antecedent", explode(col("antecedent")))

    //rules.foreach(r => calculateEWS(r))

    calculateEWS(rules.first())*/
    *
    */