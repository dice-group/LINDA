package org.aksw.dice.linda.utils

import com.google.common.collect.{ HashBiMap, BiMap }
import collection.mutable.{ HashMap, MultiMap, Set }
import org.apache.spark.sql.{ DataFrame, SQLContext, SparkSession, _ }
import org.aksw.dice.linda.datastructure.UnaryPredicate
import org.slf4j.LoggerFactory

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
    val kbrows = kb.collect()
    for (row <- kbrows) {
      var subject = row.getString(0)
      var predicate = row.getString(1)
      var obj = row.getString(2)

      var predObj = new UnaryPredicate(-1, predicate, obj)

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

        id2Subject = subject2Id.inverse()
        id2Operator = operator2Ids.inverse()

      }
    }

  }
  
  def writeRulestoFile() {
    import java.io._
    val file = new File("Horn Rules!!")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("")
    bw.close()
  }

}