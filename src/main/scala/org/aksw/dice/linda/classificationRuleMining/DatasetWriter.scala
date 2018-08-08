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

import org.apache.spark.sql.expressions.Window
import org.aksw.dice.linda.Utils.LINDAProperties._

object DatasetWriter {

  def main(args: Array[String]) = {
    lazy val subjectOperatorSchema = List(StructField("subject", StringType, true), StructField("operators", ArrayType(StringType, true), true))
    lazy val operatorIdSchema = List(StructField("resources", ArrayType(StringType), true))
    lazy val subjectIdSchema = List(StructField("subject", StringType, true))

    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .appName(APP_DATASET_CREATER)
      .getOrCreate()
    val context = spark.sparkContext
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(INPUT_DATASET)
    RDF2TransactionMap.readFromDF(triplesDF)
    val convertToVector = udf((array: Seq[Int]) => {
      array.distinct.toArray.sortBy(a => a)
    })
    val subjectOperatorMap = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._1, r._2.map(a => a.toString()))), StructType(subjectOperatorSchema))

    val operator2Id = spark.createDataFrame(RDF2TransactionMap.subject2Operator
      .map(r => Row(r._2.map(a => a.toString()))), StructType(operatorIdSchema))
      .withColumn("operator", explode(col("resources")))
      .drop(col("resources"))
      .groupBy("operator").count
      
      .withColumn("operatorId", row_number().over(Window.orderBy("operator")))
     

    val arrayContains = udf((array: mutable.WrappedArray[Int], value: Int) => {
      array.toSeq.contains(value)
    })

    val libsvmDataset = subjectOperatorMap.withColumn("operator", explode(col("operators")))
      .join(operator2Id, "operator").groupBy(col("subject"), col("operators"))
      .agg(collect_list(col("operatorId")).as("x")).drop("operators")
      .drop("subject")
      .withColumn("operatorsIds", convertToVector(col("x")))
    // this.operator2Id.write.mode(SaveMode.Overwrite).parquet(OPERATOR_ID_MAP)
    val numberofOperators = operator2Id.count().toInt
    val dataset = operator2Id.join(libsvmDataset, arrayContains(
      libsvmDataset.col("x"),
      operator2Id.col("operatorId")))
      .drop("x")
      .drop("operator")

    for (id <- 1 to 10) {
      val acceptedEntities = dataset.select("operatorsIds").where(col("operatorId") === id)
      if (acceptedEntities.count() != 0) {
        libsvmwriter(id, numberofOperators + 1, acceptedEntities, dataset.select("operatorsIds").where(col("operatorId") !== id))
      }
    }
    def libsvmwriter(id: Int, sizeOFVector: Int, acceptedEntities: DataFrame, nonAcceptedEntities: DataFrame) {
      val struct = StructType(List(
        StructField("label", DoubleType, false),
        StructField("features", VectorType, false)))
      spark.createDataFrame(acceptedEntities.rdd.map(x =>
        {
          val j = x.getSeq[Int](0).filter(_ != id).distinct
          Row(1.0, Vectors.sparse(sizeOFVector, j.toArray, Array.fill[Double](j.size) { 1 }))
        }).union(nonAcceptedEntities.rdd.map(y => Row(-1.0, Vectors.sparse(
        sizeOFVector, y.getSeq[Int](0).distinct.toArray, Array.fill[Double](y.getSeq[Int](0).distinct.size) { 1 })))), struct)
        .coalesce(1)
        .write.format("libsvm").mode(SaveMode.Overwrite).save(LIBSVM_DATASET + id)

    }
  }

}