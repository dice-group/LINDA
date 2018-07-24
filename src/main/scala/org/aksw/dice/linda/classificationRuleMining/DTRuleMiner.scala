package org.aksw.dice.linda.classificationRuleMining

import org.apache.spark.sql.SparkSession
import org.aksw.dice.linda.Utils.LINDAProperties._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import scala.collection.mutable
import scala.collection.JavaConverters._
import org.aksw.dice.linda.Utils.DTParser
import org.aksw.dice.linda.Utils.LINDAProperties._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object DTRuleMiner {
  var finalrules: DataFrame = _
  var subjectOperatorMap: DataFrame = _
  var operatorSubjectMap: DataFrame = _
  var newFacts: DataFrame = _

  val resultSchema = StructType(
    StructField("conf", IntegerType, true) ::
      StructField("s", StringType, true) ::
      StructField("p", StringType, true) ::
      StructField("o", StringType, true) :: Nil)

  val ruleSchema = StructType(
    StructField("body", ArrayType(StringType, true), true) ::
      StructField("negative", ArrayType(StringType, true), true) ::
      StructField("head", ArrayType(StringType, true), true) ::
      StructField("conf", DoubleType, true) :: Nil)

  var operator2Id: DataFrame = _
  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .appName(APP_DT_MINER)
      .getOrCreate()

    this.operator2Id = spark.read.format("parquet").load(OPERATOR_ID_MAP)
    this.finalrules = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], ruleSchema)
    val path = new Path(HDFS_MASTER + "/kunal/DTAlgo/FinalData/")
    val files = path.getFileSystem(new Configuration()).listFiles(path, true)

    val getBody = udf((bodyInt: Seq[Int]) => {
      {
        this.operator2Id.select("operator")
          .where(col("operatorIds").isin(bodyInt: _*))
          .rdd.map(r => r.getString(0)).collect().toList
      }
    })

    val getHead = udf((headInt: Int) => {
      {
        this.operator2Id.select("operator")
          .where(col("operatorIds") === (headInt))
          .rdd.map(r => r.getString(0)).collect().toList
      }
    })
    while (files.hasNext()) {

      val data = spark.read.format("libsvm").load(files.next().getPath.toString())

      val labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(data)
      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .fit(data)
      val Array(trainingData, testData) = data.randomSplit(Array(0.6, 0.4))
      //Train
      val dt = new DecisionTreeClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")

      // Convert indexed labels back to original labels.
      val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)
      // Chain indexers and tree in a Pipeline.
      val pipeline = new Pipeline()
        .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
      // Train model. This also runs the indexers.
      val model = pipeline.fit(trainingData)
      // Make predictions.
      val predictions = model.transform(testData)
      // Select (prediction, true label) and compute test error.
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
      val accuracy = evaluator.evaluate(predictions)
      val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
      val rules = spark.createDataFrame(DTParser.parse(treeModel, 1))
      val kk = rules.withColumn("body", getBody(col("antecedant")))
        .withColumn("negative", getBody(col("negation")))
        .withColumn("head", getHead(col("consequent")))
        .withColumn("confidence", lit(1.0))
        .drop("antecedant")
        .drop("negation")
        .drop("consequent")

      this.finalrules = this.finalrules.union(kk)
    }

    this.finalrules.show(false)
    spark.stop

  }
  def generateFacts(rule: Row): Unit = {
    val body = rule.getSeq(0)
    val head = rule.getSeq[String](1)(0)
    val ele = head.replaceAll("<", "").replaceAll(">", "").split(",")
    val confidence = rule.getDouble(2)
    val negation = rule.getString(3)

    val resultFacts = operatorSubjectMap.select(col("subjects"))
      .where(col("operator").isin(body: _*))
      .withColumn("subject", explode(col("subjects")))
      .drop("subjects")
      .intersect(subjectOperatorMap.select(col("subject"))
        .where(array_contains(col("operators"), negation)))
      .withColumn("conf", lit(confidence))
      .withColumn("p", lit(ele(0)))
      .withColumn("o", lit(ele(1)))
      .withColumnRenamed("subject", "s")

    this.newFacts = this.newFacts.union(resultFacts
      .select("conf", "s", "p", "o"))

  }

}