package org.aksw.dice.linda.ews
import org.apache.spark.sql.{ Row, SparkSession, _ }
import org.slf4j.LoggerFactory

import net.sansa_stack.rdf.spark.io.rdf._
import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, MatrixEntry }
object EWSFinder {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  var rules: DataFrame = _
  var subject2Id: DataFrame = _
  var operator2Id: DataFrame = _
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
    subject2Id = spark.read.parquet(mapsPath + "SubjectId/*")
    operator2Id = spark.read.parquet(mapsPath + "OperatorId/*")

    spark.stop
  }

  def calculateEWS(rule: Row) {

  }
}