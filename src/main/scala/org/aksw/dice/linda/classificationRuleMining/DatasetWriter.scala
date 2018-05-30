package org.aksw.dice.linda.classificationRuleMining
import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import scala.collection.mutable
import org.aksw.dice.linda.miner.datastructure.RDF2TransactionMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.linalg.Vectors

object DatasetWriter {
  var subjectOperatorMap: DataFrame = _
  var libsvmDataset: DataFrame = _
  var subject2Id: DataFrame = _
  var operator2Id: DataFrame = _
  val SPACE: String = " ";
  val COLON: String = ":";
  val input = "Data/rdf.nt"
  lazy val subjectOperatorSchema = List(StructField("subject", StringType, true), StructField("operators", ArrayType(StringType, true), true))
  lazy val operatorIdSchema = List(StructField("resources", ArrayType(StringType), true))
  lazy val subjectIdSchema = List(StructField("subject", StringType, true))
  var numberofOperators: Int = 0
  val spark = SparkSession.builder
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName("LINDA (Data Set Creater)")
    .getOrCreate()

  def main(args: Array[String]) = {

    val context = spark.sparkContext
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(input)
    RDF2TransactionMap.readFromDF(triplesDF)
    val convertToVector = udf((array: Seq[Long]) => {
      array.distinct.toArray.map(_.toInt).sortBy(a => a)
    })
    this.subjectOperatorMap = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorSchema))
      .withColumn("factConf", lit(1.0))
    this.subject2Id = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._1)), StructType(subjectIdSchema))
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
    operator2Id.limit(3).rdd.foreach(r => libsvmwriter(
      r.getLong(1),
      libsvmDataset.select("operatorsIds").where(array_contains(col("operatorsIds"), r.getLong(1))),
      libsvmDataset.select("operatorsIds").where(!array_contains(col("operatorsIds"), r.getLong(1)))))
    spark.stop

  }
  def libsvmwriter(id: Long, acceptedEntities: DataFrame, nonAcceptedEntities: DataFrame) {

    val struct = StructType(List(
      StructField("label", DoubleType, false),
      StructField("features", VectorType, false)))

    val a = spark.sqlContext.createDataFrame(acceptedEntities.rdd.map(x =>
      { Row(1.0, Vectors.sparse(this.numberofOperators, x.getSeq[Int](0).distinct.toArray, Array.fill[Double](x.getSeq[Int](0).distinct.size) { 1.0 })) })
      .union(nonAcceptedEntities.rdd.map(y => Row(-1.0, Vectors.sparse(
        this.numberofOperators, y.getSeq[Int](0).distinct.toArray, Array.fill[Double](y.getSeq[Int](0).distinct.size) { 1.0 })))), struct)
      .coalesce(1).write.format("libsvm").mode(SaveMode.Overwrite).save("/Users/Kunal/workspaceThesis/LINDA/Data/LIBSVMData/" + id)
  }

}