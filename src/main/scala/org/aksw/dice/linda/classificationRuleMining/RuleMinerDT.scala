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
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object RuleMinerDT {
  var subjectOperatorMap: DataFrame = _
  var libsvmDataset: DataFrame = _
  var subject2Id: DataFrame = _
  var operator2Id: DataFrame = _
  val input = "Data/rdf.nt"
  val SPACE: String = " ";
  val COLON: String = ":";
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
    val convertToVector = udf((array: Seq[Long]) => {
      array.toArray.map(_.toDouble)
    })

    this.subjectOperatorMap = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorSchema))
      .withColumn("factConf", lit(1.0))
    this.subject2Id = spark.createDataFrame(RDF2TransactionMap.subject2Operator.map(r => Row(r._1)), StructType(subjectIdSchema))
      .withColumn("subjectIds", monotonically_increasing_id())
    this.operator2Id = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._2.map(a => a.toString()))), StructType(operatorIdSchema))
      .withColumn("operator", explode(col("resources")))
      .withColumn("operatorIds", monotonically_increasing_id())
      .drop(col("resources"))
    this.libsvmDataset = subjectOperatorMap.withColumn("operator", explode(col("operators")))
      .join(operator2Id, "operator").groupBy(col("subject"), col("operators"))
      .agg(collect_list(col("operatorIds")).as("x")).drop("operators")
      .drop("factConf").drop("subject")
      .withColumn("operatorsIds", convertToVector(col("x"))).drop("x")
    libsvmDataset.show(false)

    //val a = operator2Id.limit(10).withColumn("libsvmresult", libsvmwriter(col("operatorIds")))
    //  a.select(col("libsvmresult")).write.format("libsvm").save("da")

    spark.stop

  }
  /*def libsvmwriter = udf((operator: Long) => {
    val acceptedEntities = libsvmDataset.select("operatorsIds").where(array_contains(col("operatorsIds"), operator))
    val nonAcceptedEntities = libsvmDataset.except(acceptedEntities)
    acceptedEntities.rdd.map(r => {
      new LabeledPoint(1.0, r.getSeq[Double](0)
      line.append("1").append("\t")
      .foreach(a => {
        line.append(a + COLON + "1").append(SPACE)
      })
      line.toString()
    }).union(nonAcceptedEntities.rdd.map(r => {
      val line: StringBuilder = new StringBuilder()
      line.append("-1").append("\t")
      r.getSeq[Double](0).foreach(a => {
        line.append(a + COLON + "1").append(SPACE)
      })
      line.toString()
    })).map(a => a.toString()).collect().toList

  })*/
}