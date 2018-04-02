package org.askw.dice.linda.miner.mining

import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.fpm.FPGrowth

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

    /* RDF2TransactionMap.readFromDF(triplesDF)
    import spark.implicits._

    var subject2OperatorRDD = context.parallelize(RDF2TransactionMap.subject2Operator.toSeq)

    var transactions = subject2OperatorRDD.map { case (k, v) => v.toArray }
    var subject2OperatorDF = subject2OperatorRDD.map { case (k, v) => (k, v.toArray) }.toDF("key", "value")
    var operator2SubjectRDD = context.parallelize(RDF2TransactionMap.operator2Subject.toSeq)

    var operator2SubjectDF = operator2SubjectRDD.map { case (k, v) => (k, v.toArray) }.toDF("key", "value")

    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.02).setMinConfidence(0.6)
    val model = fpgrowth.fit(transactions.toDF("items"))

    subject2OperatorDF.write.format("parquet").mode("overwrite").save("Data/Maps/Subject2OperatorMap")
    operator2SubjectDF.write.format("parquet").mode("overwrite").save("Data/Maps/Operator2SubjectMap")
    model.associationRules.write.format("parquet").mode("overwrite").save("Data/rule")*/
    spark.stop
  }

}