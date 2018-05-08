
package org.aksw.dice.linda.miner.datastructure

import collection.mutable.{ HashMap }
import org.apache.spark.sql.{ DataFrame, _ }
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD

object RDF2TransactionMap {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)
  var operator2Id = new HashMap[String, Long]
  var subject2Id = new HashMap[String, Long]

  var subject2Operator: RDD[(String, List[UnaryPredicate])] = _
  var read: RDD[(String, Iterable[Row])] = _

 

  def readFromDF(kb: DataFrame) {
    read = kb.rdd.groupBy(r => r.getString(0))
    //TODO: Change the mapping to Dataset instead of RDD.
    this.subject2Operator = read.map(r => (r._1, createUnaryPredicate(r._2)))

  }

  def createUnaryPredicate(r: Iterable[Row]): List[UnaryPredicate] = {
    var a = new ListBuffer[UnaryPredicate]()
    val x = r.iterator
    while (x.hasNext) {
      var triple = x.next()
      var predObj = new UnaryPredicate(triple.getString(1), triple.getString(2))
      a += predObj
    }

    a.toList
  }

}