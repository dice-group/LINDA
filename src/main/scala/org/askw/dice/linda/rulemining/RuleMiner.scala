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
    RDF2TransactionMap.readFromDF(triplesDF)

    val operatorRDD = context.parallelize(RDF2TransactionMap.operatorList.toList)
    var subject2operatorIdsRDD = context.parallelize(RDF2TransactionMap.subject2Operator.toSeq)
    import spark.implicits._
    var transactions = subject2operatorIdsRDD.map { case (k, v) => v.toArray }
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.02).setMinConfidence(0.6)
    val model = fpgrowth.fit(transactions.toDF("items"))

    model.freqItemsets.show()
    model.associationRules.write.format("json").mode("overwrite").save("Data/rule")
    spark.stop
  }

}