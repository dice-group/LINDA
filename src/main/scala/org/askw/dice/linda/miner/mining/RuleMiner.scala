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

object RuleMiner {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)
  val input = "Data/rdf.nt"
  var rules: DataFrame = _
  var subjectOperatorMap: DataFrame = _
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
    var subjetct2Operator = spark.createDataFrame(RDF2TransactionMap.subject2Operator.map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorSchema))
    var subjects = RDF2TransactionMap.subject2Operator.map(r => Row(r._1))
    var subject2Id = spark.createDataFrame(subjects, StructType(resourceIdSchema)).withColumn("id", row_number().over(Window.orderBy("resource")))
    var transactionsRDD = RDF2TransactionMap.subject2Operator.map(r => r._2.map(a => a.toString()))
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.02).setMinConfidence(0.5)
    import spark.implicits._
    val model = fpgrowth.fit(transactionsRDD.toDF("items"))
    var frequentOperator2Id = model.freqItemsets.drop("freq").map(r => r.getAs[Seq[String]]("items")).flatMap(a => a).withColumn("id", row_number().over(Window.orderBy("value")))

    // Writers
    model.associationRules.write.format("parquet").mode("overwrite").save("Data/rules")

    subject2Id.write.format("parquet").mode("overwrite").save("Data/Maps/SubjectId")
    frequentOperator2Id.write.format("json").mode("overwrite").save("Data/Maps/OperatorId")

    subjetct2Operator.write.format("parquet").mode("overwrite").save("Data/Maps/SubjectOperatorMap")

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