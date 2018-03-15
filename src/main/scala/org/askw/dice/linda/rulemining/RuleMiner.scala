package org.askw.dice.linda.rulemining

import org.apache.spark.sql.{ SparkSession, _ }
import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import org.aksw.dice.linda.utils.RDF2TransactionMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.fpm.FPGrowth

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
    var transactions = subject2operatorIdsRDD.map { case (k, v) => v.toArray }
    val fpg = new FPGrowth()
      .setMinSupport(0.02)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

    model.freqItemsets.take(5).foreach { itemset =>
      println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
    }

    val minConfidence = 0.02
    model.generateAssociationRules(minConfidence).take(5).foreach { rule =>
      println(s"${rule.antecedent.mkString("[", ",", "]")}============> " +
        s"${rule.consequent.mkString("[", ",", "]")},${rule.confidence}")
    }

    spark.stop
  }

}