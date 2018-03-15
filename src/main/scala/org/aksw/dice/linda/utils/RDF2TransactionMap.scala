package org.aksw.dice.linda.utils

import com.google.common.collect.{ HashBiMap, BiMap }
import collection.mutable.{ HashMap, MultiMap, Set }
import org.apache.spark.sql.{ DataFrame, SQLContext, SparkSession, _ }
import org.aksw.dice.linda.datastructure.UnaryPredicate
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD

object RDF2TransactionMap {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  var subject2operatorIds = new HashMap[String, Set[Int]] with MultiMap[String, Int]
  var operator2Ids: BiMap[UnaryPredicate, Int] = HashBiMap.create()
  var subject2Id: BiMap[String, Int] = HashBiMap.create()
  var id2Subject: BiMap[Int, String] = null
  var id2Operator: BiMap[Int, UnaryPredicate] = null
  var itemId: Int = 0
  var subjectId: Int = 0

  def readFromDF(kb: DataFrame) {
    kb.rdd.map(row => writeToMaps(row.getString(0), row.getString(1), row.getString(2)))
  }

  def writeToMaps(subject: String, pred: String, obj: String) {
    var predObj = new UnaryPredicate(-1, pred, obj)
    if (!operator2Ids.containsKey(predObj)) {
      this.itemId += 1
      operator2Ids.put(predObj, itemId)
    }
    var id = operator2Ids.get(predObj)
    predObj.id = id
    subject2operatorIds.addBinding(subject, id)
    if (!subject2Id.containsKey(subject)) {
      subject2Id.put(subject, this.synchronized {
        this.subjectId = this.subjectId + 1
        subjectId
      })
    }
  }

  def createInverseMaps() {
    this.id2Operator = this.operator2Ids.inverse()
    this.id2Subject = this.subject2Id.inverse()
  }

  def writeRulestoFile() {
    import java.io._
    val file = new File("Horn Rules!!")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("")
    bw.close()
  }

}