package org.aksw.dice.linda.classificationRuleMining
import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import scala.collection.mutable

import org.aksw.dice.linda.miner.datastructure.RDF2TransactionMap
import scala.collection.mutable.ListBuffer

object RuleMinerDT {
  var subjectOperatorMap: DataFrame = _
  var subject2Id: DataFrame = _
  var operator2Id: DataFrame = _
  val input = "Data/rdf.nt"
  lazy val subjectOperatorSchema = List(StructField("subject", StringType, true), StructField("operators", ArrayType(StringType, true), true))
  lazy val operatorIdSchema = List(StructField("resources", ArrayType(StringType), true))
  lazy val subjectIdSchema = List(StructField("subject", StringType, true))

  def main(args: Array[String]) = {

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("LINDA (Miner)")
      .getOrCreate()
    val context = spark.sparkContext
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(input)
    RDF2TransactionMap.readFromDF(triplesDF)

    this.subjectOperatorMap = spark.createDataFrame(RDF2TransactionMap.subject2Operator.map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorSchema)).withColumn("factConf", lit(1.0))
    this.subject2Id = spark.createDataFrame(RDF2TransactionMap.subject2Operator.map(r => Row(r._1)), StructType(subjectIdSchema)).withColumn("id", monotonically_increasing_id())
    this.operator2Id = spark.createDataFrame(RDF2TransactionMap.subject2Operator.map(r => Row(r._2.map(a => a.toString()))), StructType(operatorIdSchema)).withColumn("operator", explode(col("resources"))).withColumn("id", monotonically_increasing_id()).drop(col("resources"))
    val libsvmDataset = subjectOperatorMap.withColumn("operator", explode(col("operators")))
      .join(operator2Id, "operator").drop("factConf").groupBy(col("subject"), col("operators")).agg(collect_list(col("id"))).drop("operators").join(subject2Id, "subject")

    //.groupBy(col("subject"), col("list_of_ids")).agg(collect_list($"desc"))

    spark.stop
  }

}