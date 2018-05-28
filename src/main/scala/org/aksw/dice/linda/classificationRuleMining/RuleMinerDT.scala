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
  var numberofOperators: Int = 0

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
      array.toArray.map(_.toInt)
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
    this.numberofOperators = this.operator2Id.count().toInt
    this.libsvmDataset = subjectOperatorMap.withColumn("operator", explode(col("operators")))
      .join(operator2Id, "operator").groupBy(col("subject"), col("operators"))
      .agg(collect_list(col("operatorIds")).as("x")).drop("operators")
      .drop("factConf").drop("subject")
      .withColumn("operatorsIds", convertToVector(col("x"))).drop("x")
    val a = operator2Id.limit(10).rdd.foreach(r =>
      libsvmwriter(
        r.getLong(1),
        libsvmDataset.select("operatorsIds").where(array_contains(col("operatorsIds"), r.getLong(1))),
        libsvmDataset.select("operatorsIds").where(!array_contains(col("operatorsIds"), r.getLong(1)))))

    spark.stop

  }
  def libsvmwriter(id: Long, acceptedEntities: DataFrame, nonAcceptedEntities: DataFrame) {
    val a = acceptedEntities.rdd.map(x =>
      //      val nCols = r.getSeq[Int](0).max +
      LabeledPoint(1.0, Vectors.sparse(
        this.numberofOperators,
        x.getSeq[Int](0).toArray, Array.fill[Double](x.getSeq[Int](0).size) { 1.0 })))
      .union(nonAcceptedEntities.rdd.map(y =>
        LabeledPoint(0.0, Vectors.sparse(
          this.numberofOperators,
          y.getSeq[Int](0).toArray, Array.fill[Double](y.getSeq[Int](0).size) { 1.0 }))))
    MLUtils.saveAsLibSVMFile(a, "/Users/Kunal/workspaceThesis/LINDA/Data/LIBSVM" + id)
  }
}