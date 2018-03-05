package org.aksw.dice.linda.reader

import java.io.File

import org.apache.spark.sql.{ DataFrame, SparkSession, _ }

import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang

import org.apache.spark.SparkContext
import net.sansa_stack.rdf.spark.io.rdf._

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
    triplesDF.collect().foreach(println)

    sparkSession.stop

  }
}