
package org.askw.dice.linda.miner.mining

import collection.mutable.{ HashMap, MultiMap, Set }
import org.apache.spark.sql.{ DataFrame, _ }
import org.aksw.dice.linda.miner.datastructure.UnaryPredicate
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer

object RDF2TransactionMap {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)
  var operatorList = new ListBuffer[UnaryPredicate]
  var subject2Operator = new HashMap[String, Set[String]] with MultiMap[String, String]
  var operator2Subject = new HashMap[String, Set[String]] with MultiMap[String, String]

  def readFromDF(kb: DataFrame) {
    kb.distinct().foreach(row => (writeToMaps(row.getString(0), row.getString(1), row.getString(2))))
  }

  def writeToMaps(subject: String, pred: String, obj: String) {
    var predObj = new UnaryPredicate(pred, obj)
    operatorList += predObj
    subject2Operator.addBinding(subject, predObj.toString())
    operator2Subject.addBinding(predObj.toString(), subject)
  }

}