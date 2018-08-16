package org.aksw.dice.linda.classificationRuleMining
import org.apache.spark.sql.SparkSession
import org.aksw.dice.linda.Utils.LINDAProperties._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import scala.collection.mutable

object DTFactGenerator {
  def main(args: Array[String]) = {

    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .appName(APP_DT_FACTS)
      .getOrCreate()
    if (args.length == 0) {
      println("No Parameters provided")
      spark.stop()
    }

    var DATASET_NAME = args(0)
    var HDFS_MASTER = args(1)
    var INPUT_DATASET_OPERATOR_SUBJECT_MAP = HDFS_MASTER + DATASET_NAME + "/Maps/OperatorSubjectMap/"
    var FACTS_KB_DT = HDFS_MASTER + "DT/" + DATASET_NAME + "/Facts"

    var DT_RULES_RAW = HDFS_MASTER + "DT/" + DATASET_NAME + "/Rules/RawIds"
    var DT_RULES = HDFS_MASTER + "DT/" + DATASET_NAME + "/Rules/FinalRules"
    var DT_FACTS = HDFS_MASTER + "DT/" + DATASET_NAME + "/Facts"

    val operatorSubjectMap = spark.read.json(INPUT_DATASET_OPERATOR_SUBJECT_MAP)

    var ruleWithNames = spark.read.json(DT_RULES_RAW)
      .withColumnRenamed("operatorIds", "consequent")

    def flatten = udf((xs: Seq[Seq[String]]) => xs.flatten.distinct)
    def getFacts = udf((body: mutable.WrappedArray[String], head: mutable.WrappedArray[String], neg: mutable.WrappedArray[String]) => {
      body.union(neg).diff(head)
    })
    val rulesWithFacts = ruleWithNames.join(
      operatorSubjectMap,
      ruleWithNames.col("body") === operatorSubjectMap.col("operator"))
      .withColumnRenamed("facts", "bodySet")
      .drop("operator")
      .join(
        ruleWithNames.join(operatorSubjectMap, ruleWithNames("head") === operatorSubjectMap("operator"))
          .withColumnRenamed("facts", "headSet")
          .drop("operator"), Seq("antecedant", "consequent", "negation", "body", "negative", "head"))
      .join(
        ruleWithNames.join(operatorSubjectMap, ruleWithNames("negative")
          <=> operatorSubjectMap("operator")),
        Seq("antecedant", "consequent", "negation", "body", "negative", "head"), "outer")
      .withColumnRenamed("facts", "negativeSet")
      .groupBy("antecedant", "consequent", "negation", "head")
      .agg(
        collect_list("body").as("ruleBody"),
        collect_list("negative").as("ruleNeg"),
        flatten(collect_list("bodySet")).as("bodyFacts"),
        flatten(collect_list("negativeSet")).as("negFacts"),
        flatten(collect_list("headSet")).as("headFacts"))
      .withColumn("Facts", getFacts(col("bodyFacts"), col("headFacts"), col("negFacts")))

    val removeEmpty = udf((array: Seq[String]) => !array.isEmpty)

    rulesWithFacts.select(col("Facts"), col("head")).
      filter(removeEmpty(col("Facts")))
      .withColumn("newFacts", explode(col("Facts")))
      .withColumn("conf", lit(1))
      .withColumn("_tmp", split(col("head"), "\\::"))
      .withColumn("triple", concat(
        col("newFacts"), lit(" "),
        col("_tmp").getItem(0).as("p"), lit(" "),
        col("_tmp").getItem(1).as("o")))
      .select("conf", "triple")
      .write.mode(SaveMode.Overwrite).json(DT_FACTS)

    rulesWithFacts
      .select("ruleBody", "ruleNeg", "head")
      .write.mode(SaveMode.Overwrite).json(DT_RULES)

    spark.stop
  }
}