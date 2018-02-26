package org.aksw.dice.linda.reader


import java.io.File

import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.model.JenaSparkRDDOps
import org.apache.jena.graph.Node_URI
import net.sansa_stack.rdf.spark.model.TripleRDD
import org.apache.jena.graph.Node_Literal
import org.apache.jena.sparql.util.NodeFactoryExtra
import net.sansa_stack.rdf.spark.io.NTripleReader

object TripleReader {

  def main(args: Array[String]) = {
    val input = "Data/rdf.nt"

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader example (" + input + ")")
      .getOrCreate()

    val triples = sparkSession.sparkContext.textFile(input)
    triples.take(5).foreach(println(_))
    triples.cache()

    val nrOfTriples = triples.count()
    println("Count: " + nrOfTriples)

    val triplesRDD = NTripleReader.load(sparkSession, new File(input))

    val graph: TripleRDD = triplesRDD

    //println("Number of triples: " + graph.find(ANY, ANY, ANY).distinct.count())
    println("Number of subjects: " + graph.getSubjects.distinct.count())
    println("Number of predicates: " + graph.getPredicates.distinct.count())
    println("Number of objects: " + graph.getPredicates.distinct.count())

    sparkSession.stop

  }

}