package org.aksw.dice.linda.ews
import org.apache.spark.sql.{ Row, SparkSession, Encoder, _ }
import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable
import org.apache.spark.sql.functions.col
object EWSFinder {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  var rules: DataFrame = _
  var subject2OperatorDF: DataFrame = _
  var operator2SubjectDF: DataFrame = _
  val spark = SparkSession.builder
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName("LINDA (EWS Finder)")
    .getOrCreate()

  def main(args: Array[String]) = {
    val rulesPath = "Data/rule/*"
    val mapsPath = "Data/Maps/"

    val context = spark.sparkContext

    rules = spark.read.parquet(rulesPath)
    subject2OperatorDF = spark.read.parquet(mapsPath + "Subject2OperatorMap/*")
    operator2SubjectDF = spark.read.parquet(mapsPath + "Operator2SubjectMap/*")
    rules.foreach(rule => calculateEWS(rule))
    spark.stop
  }

  def calculateEWS(rule: Row) {
    val head = rule.getAs[mutable.WrappedArray[String]](0)
    val body = rule.getAs[mutable.WrappedArray[String]](1)
    var headFacts = operator2SubjectDF.select(operator2SubjectDF("value")) filter (operator2SubjectDF("key").isin(head: _*))
    var bodyFacts = operator2SubjectDF.select(operator2SubjectDF("value")).filter(operator2SubjectDF("key").isin(body: _*))
    var NS = headFacts.intersect(bodyFacts)
    var ABS = bodyFacts.except(headFacts)
    NS.show()
    ABS.show()
  }
}