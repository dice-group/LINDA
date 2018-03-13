package org.askw.dice.linda.rulemining

import org.apache.spark.sql.{ SparkSession, _ }
import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import org.aksw.dice.linda.utils.RDF2TransactionMap
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
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(input)
    RDF2TransactionMap.readFromDF(triplesDF)
    import spark.implicits._
    val transactionscount = RDF2TransactionMap.subject2operatorIds

    var transactions = new ListBuffer[String]()
    for ((k, v) <- transactionscount) {
      transactions += v.mkString(" ")
    }
    this.logger.info(" Total number of Unary operators " + RDF2TransactionMap.operator2Ids.size())
    this.logger.info(" Total number of Subjects " + RDF2TransactionMap.id2Subject.keySet().size())
    this.logger.info("Total number of transactions " + transactionscount.keySet.size)
    val dataset = spark.createDataset(transactions).map(t => t.split(" ")).toDF("items")
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0).setMinConfidence(0.01)
    val model = fpgrowth.fit(dataset)

    model.associationRules.show()

    spark.stop
  }

}