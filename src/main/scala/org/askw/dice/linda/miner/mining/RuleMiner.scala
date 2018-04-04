package org.askw.dice.linda.miner.mining

import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import org.apache.spark.sql.expressions.Window;
import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.functions;
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.aksw.dice.linda.miner.datastructure.UnaryPredicate

object RuleMiner {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]) = {
    val input = "Data/rdf.nt"

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("LINDA (Miner)")
      .getOrCreate()
    val context = spark.sparkContext
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(input)
    RDF2TransactionMap.readFromDF(triplesDF)
    val someSchema = List(StructField("resource", StringType, true))
    var subjects = RDF2TransactionMap.subject2Operator.map(r => Row(r._1))
    var operators = RDF2TransactionMap.subject2Operator.map(r => r._2.toList.distinct).flatMap(y => y).distinct().map(a => Row(a.toString()))

    var subject2Id = spark.createDataFrame(subjects, StructType(someSchema)).withColumn("id", functions.row_number().over(Window.orderBy("resource")))
    var operator2Id = spark.createDataFrame(operators, StructType(someSchema)).withColumn("id", functions.row_number().over(Window.orderBy("resource")))
    var transactionsRDD = RDF2TransactionMap.subject2Operator.map(r => r._2.toArray.map(a => a.toString()))
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.02).setMinConfidence(0.6)
    import spark.implicits._
    val model = fpgrowth.fit(transactionsRDD.toDF("items"))
    model.freqItemsets
    model.associationRules.show()

    /*





    subject2OperatorDF.write.format("parquet").mode("overwrite").save("Data/Maps/Subject2OperatorMap")
    operator2SubjectDF.write.format("parquet").mode("overwrite").save("Data/Maps/Operator2SubjectMap")
    model.associationRules.write.format("parquet").mode("overwrite").save("Data/rule")*/
    spark.stop
  }

}