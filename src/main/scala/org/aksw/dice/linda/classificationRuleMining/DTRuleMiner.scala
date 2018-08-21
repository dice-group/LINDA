package org.aksw.dice.linda.classificationRuleMining

import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import org.aksw.dice.linda.Utils.DTParser
import org.aksw.dice.linda.Utils.LINDAProperties._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object DTRuleMiner {

  def main(args: Array[String]) = {

    val ruleSchema = StructType(
      StructField("antecedant", ArrayType(IntegerType, true), true) ::
        StructField("consequent", IntegerType, true) ::
        StructField("negation", ArrayType(IntegerType, true), true) :: Nil)
    val operatorSchema = StructType(
      StructField("operator", StringType, true) ::
        StructField("operatorIds", IntegerType, true) :: Nil)

    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .appName(APP_DT_MINER)
      .getOrCreate()

    if (args.length == 0) {
      println("No Parameters provided")
      spark.stop()
    }

    var DATASET_NAME = args(0)
    var HDFS_MASTER = args(1)

    var DT_RULES_RAW = HDFS_MASTER + "DT/" + DATASET_NAME + "/Rules/RawIds"

    var DT_INPUT_DATASET = HDFS_MASTER + "DT/" + DATASET_NAME + "/LIBSVMDATA/"
    var DT_OPERATOR_ID = HDFS_MASTER + DATASET_NAME + "/operator2Id.csv"
    var rulesWithId = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], ruleSchema)
    val path = new Path(DT_INPUT_DATASET)
    val files = path.getFileSystem(new Configuration()).listFiles(path, true)
    val operator2Id = spark.read.schema(operatorSchema).csv(DT_OPERATOR_ID).cache()

    breakable {
      while (files.hasNext()) {
        val filename = files.next().getPath.toString()
        if (!filename.contains(".libsvm"))
          break
        val data = spark.read.format("libsvm").load(filename)
        val labelIndexer = new StringIndexer()
          .setInputCol("label")
          .setOutputCol("indexedLabel")
          .fit(data)
        val featureIndexer = new VectorIndexer()
          .setInputCol("features")
          .setOutputCol("indexedFeatures")
          .fit(data)
        val Array(trainingData, testData) = data.randomSplit(Array(0.6, 0.4))
        val dt = new DecisionTreeClassifier()
          .setLabelCol("indexedLabel")
          .setFeaturesCol("indexedFeatures")
        val labelConverter = new IndexToString()
          .setInputCol("prediction")
          .setOutputCol("predictedLabel")
          .setLabels(labelIndexer.labels)
        val pipeline = new Pipeline()
          .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
        val model = pipeline.fit(trainingData)
        // Make predictions.
        val predictions = model.transform(testData)
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("indexedLabel")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
        val rules = spark.createDataFrame(DTParser.parse(treeModel, 1))
        rulesWithId = rulesWithId.union(rules)
      }
    }
    println("Number of Rules " + rulesWithId.count())

    val rulesWithBody = rulesWithId.withColumn("operatorIds", explode(col("antecedant")))
      .join(operator2Id, "operatorIds")
      .withColumnRenamed("operator", "head")
      .drop("operatorIds")

    val rulesWithNegation = rulesWithId.withColumn("operatorIds", explode(col("negation")))
      .join(operator2Id, "operatorIds")
      .withColumnRenamed("operator", "body")
      .drop("operatorIds")

    val rulesWithBodyandHead = rulesWithBody.join(rulesWithNegation, Seq("antecedant", "consequent", "negation"))
      .withColumnRenamed("consequent", "operatorIds")
    val rulesWithNames = rulesWithBodyandHead
      .join(operator2Id, "operatorIds")
      .withColumnRenamed("operator", "negative")

    rulesWithNames.write.mode(SaveMode.Overwrite).json(DT_RULES_RAW)
    spark.stop

  }

}