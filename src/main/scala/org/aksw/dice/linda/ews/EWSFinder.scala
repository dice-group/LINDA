package org.aksw.dice.linda.ews
import org.apache.spark.sql.{ Row, SparkSession, _ }
import org.slf4j.LoggerFactory
import scala.collection.mutable
import net.sansa_stack.rdf.spark.io.rdf._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, MatrixEntry }
object EWSMining {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  var rules: DataFrame = _
  var subjectOperatorMap: DataFrame = _
  var operator2Id: DataFrame = _
  val spark = SparkSession.builder
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName("LINDA (EWS Finder)")
    .getOrCreate()

  def main(args: Array[String]) = {
    val rulesPath = "Data/rules/*"
    val mapsPath = "Data/Maps/"

    val context = spark.sparkContext

    var a = spark.read.parquet(rulesPath)
    subjectOperatorMap = spark.read.parquet(mapsPath + "SubjectOperatorMap/*")
    rules = a.withColumn("antecedent", explode(col("antecedent")))

    //rules.foreach(r => calculateEWS(r))
    //calculateEWS(rules.first())
    spark.stop
  }

  def calculateEWS(rule: Row) {

    val head = rule.getString(0)
    val body = rule.getAs[mutable.WrappedArray[String]](1)

    var headFacts = subjectOperatorMap.select(subjectOperatorMap("subject"), subjectOperatorMap("operators")).filter(subjectOperatorMap("operators").isin(head: _*))
    var bodyFacts = subjectOperatorMap.select(subjectOperatorMap("subject"), subjectOperatorMap("operators")).filter(subjectOperatorMap("operators").isin(body: _*))
    headFacts.show()
    bodyFacts.show()
  }
}