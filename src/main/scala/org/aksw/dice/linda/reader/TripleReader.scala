package org.aksw.dice.linda.reader

import java.io.File

import org.apache.spark.sql.{ DataFrame, SparkSession, _ }

import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang

import org.apache.spark.SparkContext
import net.sansa_stack.rdf.spark.io.rdf._
import org.aksw.dice.linda.utils.RDF2TransactionMap


object TripleReader {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]) = {
    val input = "Data/rdf.nt"

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("LINDA (" + input + ")")
      .getOrCreate()

    val triplesDF = sparkSession.read.rdf(Lang.NTRIPLES)(input)
    val transactionMap = new RDF2TransactionMap()
    transactionMap.readFromDF(triplesDF)
    val transactions = transactionMap.subject2operatorIds
    this.logger.info(" Total number of Unary operators " + transactionMap.operator2Ids.size())
    this.logger.info("Total number of transactions " + transactions.keySet.size)
    transactions.foreach(x => println(x._1 + "-->" + x._2))
    sparkSession.stop

  }
}