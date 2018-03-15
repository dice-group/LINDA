package org.aksw.dice.linda.utils

import com.google.common.collect.{ HashBiMap, BiMap }
import collection.mutable.{ HashMap, MultiMap, Set }
import org.apache.spark.sql.{ DataFrame, SQLContext, SparkSession, _ }
import org.aksw.dice.linda.datastructure.UnaryPredicate
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD

object RDF2TransactionMap {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  var subject2Operator = new HashMap[String, Set[String]] with MultiMap[String, String]

  def readFromDF(kb: DataFrame) {

    kb.distinct().foreach(row => (writeToMaps(row.getString(0), row.getString(1), row.getString(2))))
  }

  def writeToMaps(subject: String, pred: String, obj: String) {
    var predObj = new UnaryPredicate(pred, obj)

    subject2Operator.addBinding(subject, predObj.toString())

  }

  def writeRulestoFile() {
    import java.io._
    val file = new File("Horn Rules!!")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("")
    bw.close()
  }

}