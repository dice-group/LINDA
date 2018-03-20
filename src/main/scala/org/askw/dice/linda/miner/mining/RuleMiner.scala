package org.askw.dice.linda.miner.mining

import org.apache.spark.sql.{ SparkSession, _ }
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
      .appName("LINDA (" + input + ")")
      .getOrCreate()
    val context = spark.sparkContext
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(input)
    RDF2TransactionMap.readFromDF(triplesDF)
    var subject2operatorIdsRDD = context.parallelize(RDF2TransactionMap.subject2Operator.toSeq)
    import spark.implicits._
    var transactions = subject2operatorIdsRDD.map { case (k, v) => v.toArray }
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.02).setMinConfidence(0.6)
    val model = fpgrowth.fit(transactions.toDF("items"))
    val operatorRDD = context.parallelize(RDF2TransactionMap.operatorList.toList)

    model.associationRules.write.format("json").mode("overwrite").save("Data/rule")
    spark.stop
  }

}