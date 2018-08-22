package org.aksw.dice.linda.Utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.expressions.Window;
import org.slf4j.LoggerFactory
import org.apache.spark.ml.fpm.FPGrowth
import scala.collection.mutable

import org.apache.spark.sql.SaveMode

object DatasetParser {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master(LINDAProperties.SPARK_SYSTEM)
      .config(LINDAProperties.SERIALIZER, LINDAProperties.KYRO_SERIALIZER)
      .appName(LINDAProperties.APP_DATASET_PROCESSOR)
      .getOrCreate()

    if (args.length == 0) {
      println("No Parameters provided")
      spark.stop()
    }
    var DATASET_NAME = args(0)
    var HDFS_MASTER = args(1)
    var INPUT_DATASET = args(2)

    var INPUT_DATASET_SUBJECT_OPERATOR_MAP = HDFS_MASTER + DATASET_NAME + "/Maps/SubjectOperatorMap/"
    var INPUT_DATASET_OPERATOR_SUBJECT_MAP = HDFS_MASTER + DATASET_NAME + "/Maps/OperatorSubjectMap/"
    var HORN_RULES = HDFS_MASTER + DATASET_NAME + "/Rules/"

    val triplesDF =
      spark.createDataFrame(spark.sparkContext.textFile(INPUT_DATASET)
        .filter(!_.startsWith("#")).map(data => TripleUtils.parsTriples(data)))
        .withColumn(
          "unaryOperator",
          concat(col("predicate"), lit("::<"), col("object")))
    def getSupport = udf((head: mutable.WrappedArray[String]) => {
      head.size
    })
    println(DATASET_NAME + "::::  Number of Triples :  " + triplesDF.count())

    val subjectOperatorMap = triplesDF.groupBy(col("subject"))
      .agg(collect_set(col("unaryOperator")).as("operators"))
    val operatorSubjectMap = triplesDF.groupBy(col("unaryOperator"))
      .agg(collect_set(col("subject")).as("facts"))
      .withColumnRenamed("unaryOperator", "operator")
      .withColumn("support", getSupport(col("facts")))
    val operatorSupport = 0.05 * operatorSubjectMap.count

    val fpgrowth = new FPGrowth().setItemsCol("items")
      .setMinSupport(0.01)
      .setMinConfidence(0.6)
    val model = fpgrowth.fit(subjectOperatorMap
      .select(col("operators").as("items")))

    val originalRules = model.associationRules
    println("Number of Original Rules " + originalRules.count())
    val removeEmpty = udf((array: Seq[String]) => !array.isEmpty)

    /*
     * HORN RULE MINER
     *  NOT TO BE EXECUTED FOR DT
*/

    val hornRules = originalRules
      .filter(removeEmpty(col("consequent")))
      .withColumn("body", explode(col("antecedent")))
      .withColumn("head", explode(col("consequent")))

    hornRules.coalesce(1).write.mode(SaveMode.Overwrite).json(HORN_RULES)
    //END OF HORN RULE Miner

    subjectOperatorMap.write.mode(SaveMode.Overwrite).json(INPUT_DATASET_SUBJECT_OPERATOR_MAP)
    operatorSubjectMap.filter(col("support") > operatorSupport)
      .drop("support")
      .write.mode(SaveMode.Overwrite)
      .json(INPUT_DATASET_OPERATOR_SUBJECT_MAP)
    spark.stop

  }
}