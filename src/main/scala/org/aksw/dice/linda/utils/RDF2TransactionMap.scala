package org.aksw.dice.linda.utils

import com.google.common.collect.{ HashBiMap, BiMap }
import collection.mutable.{ HashMap, MultiMap, Set }
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, _}
import org.aksw.dice.linda.datastructure.UnaryPredicate
import net.sansa_stack.ml.spark.mining.amieSpark.RDFTriple

class RDF2TransactionMap {

  val subject2operatorIds = new HashMap[String, Set[Int]] with MultiMap[String, Int]
  val operator2Ids: HashBiMap[String, Int] = HashBiMap.create()
  val subject2Id: BiMap[String, Int] = HashBiMap.create()
  val id2Subject: BiMap[Int, String] = null
  val id2Operator: BiMap[Int, UnaryPredicate] = null
  val itemId: Double = 0
  val subjectId: Double = 0


  
}