
package org.askw.dice.linda.miner.mining

import collection.mutable.{ HashMap, MultiMap, Set }
import org.apache.spark.sql.{ DataFrame, _ }
import org.aksw.dice.linda.miner.datastructure.UnaryPredicate
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD

object RDF2TransactionMap {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)
  var operator2Id = new HashMap[String, Long]
  var subject2Id = new HashMap[String, Long]

  //Temporary
  var subject2Operator: RDD[(String, Iterable[Row])] = _
  var operator2Subject = new HashMap[String, Set[String]] with MultiMap[String, String]

  def readFromDF(kb: DataFrame) {
    subject2Operator = kb.rdd.groupBy(r => r.getString(0))
    var x = subject2Operator.map(r => (r _1, createUnaryPredicate(r._2)))
    x.foreach(println)

  }

  def createUnaryPredicate(r: Iterable[Row]): List[UnaryPredicate] = {
    var a = new ListBuffer[UnaryPredicate]()
    val x = r.iterator
    while (x.hasNext) {
      var triple = x.next()
      a += new UnaryPredicate(triple.getString(1), triple.getString(2))
    }
    a.toList
  }
  def writeToMaps(subject: String, pred: String, obj: String) {
    var predObj = new UnaryPredicate(pred, obj)

  }

}