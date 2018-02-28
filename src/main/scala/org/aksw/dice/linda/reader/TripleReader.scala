package org.aksw.dice.linda.reader

import java.io.File

import org.apache.spark.sql.{ DataFrame, SparkSession, _ }

import org.slf4j.LoggerFactory
import org.aksw.dice.linda.datastructure.RDFTriple
import org.apache.spark.SparkContext

object TripleReader {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  case class Atom(rdf: RDFTriple)

  def loadFromFileDF(path: String, sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val startTime = System.currentTimeMillis()
    import sqlContext.implicits._

    val triples = sc.textFile(path)
      .map(line => line.replace("<", "").replace(">", "").split("\\s+")) // line to tokens
      .map(tokens => Atom(RDFTriple(tokens(0), tokens(1), tokens(2).stripSuffix(".")))) // tokens to triple
      .toDF()

    logger.info("finished loading DS " + triples.count() + " triples in " + (System.currentTimeMillis() - startTime) + "ms.")
    
    return triples
  }

  def main(args: Array[String]) = {
    val input = "Data/rdf.nt"

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader example (" + input + ")")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    loadFromFileDF(input, sc, sqlContext)
    sparkSession.stop

  }
}