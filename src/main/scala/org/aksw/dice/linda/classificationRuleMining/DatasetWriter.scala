package org.aksw.dice.linda.classificationRuleMining
import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import scala.collection.mutable
import org.aksw.dice.linda.Utils.RDF2TransactionMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.linalg.Vectors
import org.apache.hadoop.fs._
import org.apache.spark.sql.expressions.Window
import org.aksw.dice.linda.Utils.LINDAProperties._

object DatasetWriter {
  var subjectOperatorMap: DataFrame = _

  var libsvmDataset: DataFrame = _

  var operator2Id: DataFrame = _
  lazy val subjectOperatorSchema = List(StructField("subject", StringType, true), StructField("operators", ArrayType(StringType, true), true))
  lazy val operatorIdSchema = List(StructField("resources", ArrayType(StringType), true))
  lazy val subjectIdSchema = List(StructField("subject", StringType, true))
  var numberofOperators: Int = 0
  val spark = SparkSession.builder
    .master(SPARK_SYSTEM)
    .config(SERIALIZER, KYRO_SERIALIZER)
    .appName(APP_DATASET_CREATER)
    .getOrCreate()

  def main(args: Array[String]) = {

    val context = spark.sparkContext
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(INPUT_DATASET)
    RDF2TransactionMap.readFromDF(triplesDF)
    val convertToVector = udf((array: Seq[Int]) => {
      array.distinct.toArray.sortBy(a => a)
    })
    this.subjectOperatorMap = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorSchema))
      .withColumn("factConf", lit(1.0))

    this.operator2Id = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._2.map(a => a.toString()))), StructType(operatorIdSchema))
      .withColumn("operator", explode(col("resources")))
      .withColumn("operatorIds", row_number().over(Window.orderBy("operator")))
      .drop(col("resources"))

    this.numberofOperators = this.operator2Id.count().toInt
    this.libsvmDataset = subjectOperatorMap.withColumn("operator", explode(col("operators")))
      .join(operator2Id, "operator").groupBy(col("subject"), col("operators"))
      .agg(collect_list(col("operatorIds")).as("x")).drop("operators")
      .drop("factConf").drop("subject")
      .withColumn("operatorsIds", convertToVector(col("x"))).drop("x")
    this.operator2Id.write.mode(SaveMode.Overwrite).parquet(OPERATOR_ID_MAP)
    operator2Id.limit(3).rdd.foreach(r => libsvmwriter(
      r.getInt(1),
      libsvmDataset.select("operatorsIds").where(array_contains(col("operatorsIds"), r.getInt(1))),
      libsvmDataset.select("operatorsIds").where(!array_contains(col("operatorsIds"), r.getInt(1)))))
    spark.stop

  }
  def libsvmwriter(id: Int, acceptedEntities: DataFrame, nonAcceptedEntities: DataFrame) {
    val struct = StructType(List(
      StructField("label", DoubleType, false),
      StructField("features", VectorType, false)))
    spark.sqlContext.createDataFrame(acceptedEntities.rdd.map(x =>
      {
        val j = x.getSeq[Int](0).filter(_ != id).distinct
        Row(1.0, Vectors.sparse(this.numberofOperators + 1, j.toArray, Array.fill[Double](j.size) { 1 }))
      }).union(nonAcceptedEntities.rdd.map(y => Row(-1.0, Vectors.sparse(
      this.numberofOperators + 1, y.getSeq[Int](0).distinct.toArray, Array.fill[Double](y.getSeq[Int](0).distinct.size) { 1 })))), struct)
      //TODO: Coalesce has to be changed
      .coalesce(1).write.format("libsvm").mode(SaveMode.Overwrite).save(LIBSVM_DATASET + id)

  }

}