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

import org.aksw.dice.linda.Utils.LINDAProperties._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.collection.mutable.ListBuffer

object DTRuleMiner {

  def main(args: Array[String]) = {
    val operatorSchema = StructType(
      StructField("operator", StringType, true) ::
        StructField("operatorIds", IntegerType, true) :: Nil)
    val ruleSchema = StructType(
      StructField("antecedant", ArrayType(IntegerType, true), true) ::
        StructField("consequent", IntegerType, true) ::
        StructField("negation", ArrayType(IntegerType, true), true) :: Nil)

    val spark = SparkSession.builder
      .master(SPARK_SYSTEM)
      .config(SERIALIZER, KYRO_SERIALIZER)
      .appName(APP_DT_MINER)
      .getOrCreate()
    var rulesWithId = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], ruleSchema)
    if (args.length == 0) {
      println("No Parameters provided")
      spark.stop()
    }

    var DATASET_NAME = args(0)
    var HDFS_MASTER = args(1)

    var DT_RULES_RAW = HDFS_MASTER + DATASET_NAME + "/Rules/RawIds"

    var DT_INPUT_DATASET = HDFS_MASTER + DATASET_NAME + "/LIBSVM/"
    var DT_OPERATOR_ID = HDFS_MASTER + DATASET_NAME + "/operator2Id.csv"

    val path = new Path(DT_INPUT_DATASET)
    val files = path.getFileSystem(new Configuration()).listFiles(path, true)

    val operator2Id = spark.read.schema(operatorSchema).csv(DT_OPERATOR_ID)
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
        val rules = spark.createDataFrame(parse(treeModel, 1))
        rulesWithId = rulesWithId.union(rules)
      }
    }

    println("Number of Rules " + rulesWithId.count())

    val rulesWithBody = rulesWithId.withColumn("operatorIds", explode(col("antecedant")))
      .join(operator2Id, "operatorIds")
      .withColumnRenamed("operator", "body")
      .drop("operatorIds")

    val rulesWithNegation = rulesWithId.withColumn("operatorIds", explode(col("negation")))
      .join(operator2Id, "operatorIds")
      .withColumnRenamed("operator", "negative")
      .drop("operatorIds")

    val rulesWithBodyandHead = rulesWithBody.join(rulesWithNegation, Seq("antecedant", "consequent", "negation"))
      .withColumnRenamed("consequent", "operatorIds")
    val rulesWithNames = rulesWithBodyandHead
      .join(operator2Id, "operatorIds")
      .withColumnRenamed("operator", "head")

    rulesWithNames.write.mode(SaveMode.Overwrite).json(DT_RULES_RAW)
    spark.stop

  }

  case class Rule(
    antecedant: List[Int],
    consequent: Int,
    negation:   List[Int]) {
    override def toString() = this.antecedant + " " + this.negation + " " + this.consequent
  }
  def parse(tree: DecisionTreeClassificationModel, id: Int): List[Rule] = {
    var lines = tree.toDebugString.lines.toList
    var stack = new ListBuffer[String]()
    var result = new ListBuffer[List[String]]()
    while (lines.length > 0) {
      if (lines(0).contains("If")) {
        stack.append(lines(0).trim())
      } else if (lines(0).contains("Predict")) {
        result.append(stack.toList :+ lines(0).replace(")", "").trim())
        if ((stack.length != 0) && (stack(stack.length - 1).contains("Else")))
          stack.remove(stack.length - 1)
      } else if (lines(0).contains("Else")) {
        stack.remove(stack.length - 1)
        stack.append(lines(0).trim())
      }
      lines = lines.tail
    }
    result.toList.filter(r => !r(r.size - 1).contains("Predict: 0.0")).map(r =>
      parserLine(r, id))
  }
  def parserLine(
    line: List[String],
    id:   Int): Rule = {
    var antecedant = new ListBuffer[Int]
    var negation = new ListBuffer[Int]
    line.foreach(a => {
      if (!a.contains("Predict")) {
        var content = a.substring(a.indexOf('('), a.indexOf(')')).split(" ")
        if (((content.contains("not")) && (content.contains("{0.0}")))
          || (((!content.contains("not")) && (content.contains("{1.0}"))))) {
          antecedant.append(content(1).toInt)
        } else if ((!content.contains("not")) && (content.contains("{0.0}"))
          || (((content.contains("not")) && (content.contains("{1.0}"))))) {
          negation.append(content(1).toInt)
        }
      }
    })
    //Cannot do transformation here as it is in memory
    new Rule(antecedant.toList, id, negation.toList)
  }

}