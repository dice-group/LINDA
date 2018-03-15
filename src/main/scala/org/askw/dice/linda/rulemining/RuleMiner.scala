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
    val context = spark.sparkContext
    val triplesDF = spark.read.rdf(Lang.NTRIPLES)(input)
    val a = RDF2TransactionMap.readFromDF(triplesDF)

    val subject2operatorIdsRDD = context.parallelize(RDF2TransactionMap.subject2operatorIds.toSeq)
    print(subject2operatorIdsRDD.collect().length)
    spark.stop
  }

}