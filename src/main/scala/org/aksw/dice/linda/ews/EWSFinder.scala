package org.aksw.dice.linda.ews
import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import org.slf4j.LoggerFactory
import org.apache.jena.riot.Lang
import net.sansa_stack.rdf.spark.io.rdf._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable
object EWSFinder {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  var rules: DataFrame = _
  var subject2OperatorDF: DataFrame = _
  var operator2SubjectDF: DataFrame = _

  def main(args: Array[String]) = {
    val rulesPath = "Data/rule/*"
    val mapsPath = "Data/Maps/"
    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("LINDA (EWS Finder)")
      .getOrCreate()
    val context = spark.sparkContext

    rules = spark.read.parquet(rulesPath)
    subject2OperatorDF = spark.read.parquet(mapsPath + "Subject2OperatorMap/*")
    operator2SubjectDF = spark.read.parquet(mapsPath + "Operator2SubjectMap/*")

    spark.stop
  }

  def calculateEWS(rule: Row) {
    val head = rule.getAs[mutable.WrappedArray[String]](0)
    val body = rule.getAs[mutable.WrappedArray[String]](1)

    for (ele <- head) {

    }

  }
}