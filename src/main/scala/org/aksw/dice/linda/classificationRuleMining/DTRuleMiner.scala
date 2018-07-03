package org.aksw.dice.linda.classificationRuleMining

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer }
import org.apache.spark.sql.{ SparkSession, Encoder, _ }
import scala.collection.mutable
import scala.collection.JavaConverters._

object DTRuleMiner {
  var operator2Id: DataFrame = _
  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("LINDA (DT Classifier) ")
      .getOrCreate()

    val data = spark.read.format("libsvm").load("/Users/Kunal/workspaceThesis/LINDA/Data/LIBSVMData/0/00")
    this.operator2Id = spark.read.format("parquet").load("/Users/Kunal/workspaceThesis/LINDA/Data/Maps/OperatorId")
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .fit(data)
    val Array(trainingData, testData) = data.randomSplit(Array(0.6, 0.4))
    // Train a DecisionTree model.
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
    rules.withColumn("body", getBody(col("antecedant")))
      .withColumn("negative", getBody(col("negation")))
      .withColumn("head", getHead(col("consequent")))
      .drop("antecedant")
      .drop("negation")
      .drop("consequent")
      .show(false)
    //this.operator2Id.show(false)

    spark.stop
  }

}