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

/*object DTRuleMiner {
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

    this.operatorSubjectMap = spark.read.format("parquet").load(INPUT_DATASET_OPERATOR_SUBJECT_MAP)
    this.newFacts = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], resultSchema)
    this.operator2Id = spark.read.format("parquet").load(OPERATOR_ID_MAP)
    this.finalrules = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], ruleSchema)
    val path = new Path(DT_INPUT_DATASET)
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
      val kk = rules.withColumn("body", getBody(col("antecedant")))
        .withColumn("negative", getBody(col("negation")))
        .withColumn("head", getHead(col("consequent")))
        .withColumn("confidence", lit(1.0))
        .drop("antecedant")
        .drop("negation")
        .drop("consequent")
      this.finalrules = this.finalrules.union(kk)
    }
    this.finalrules.foreach(rule => generateFacts(rule))
    this.newFacts.select(col("conf"), concat(lit("<"), col("s"), lit(">"), lit(" "), lit("<"), col("p"), lit(">"), lit(" "), lit("<"), col("o"), lit(">")))
      .coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header", "false")
      .option("delimiter", "\t").csv(FACTS_KB_DT)
    this.finalrules.write.mode(SaveMode.Overwrite).json(DT_RULES_JSON)
    spark.stop

  }
  def generateFacts(rule: Row): Unit = {
    val body = rule.getSeq(0)
    val negation = rule.getSeq(1)
    val head = rule.getSeq[String](2)(0)
    val ele = head.replaceAll("<", "").replaceAll(">", "").split(",")
    val confidence = rule.getDouble(3)
    if (negation.size == 0) {
      this.newFacts = this.newFacts.
        union(operatorSubjectMap.select(col("subjects"))
          .where(col("operator").isin(body: _*))
          .withColumn("subject", explode(col("subjects")))
          .drop("subjects")
          .withColumn("conf", lit(confidence))
          .withColumn("p", lit(ele(0)))
          .withColumn("o", lit(ele(1)))
          .withColumnRenamed("subject", "s")
          .select("conf", "s", "p", "o"))
    } else {
      this.newFacts = this.newFacts.
        union(operatorSubjectMap.select(col("subjects"))
          .where(col("operator").isin(body: _*)
            and col("operator").isin(negation: _*))
          .withColumn("subject", explode(col("subjects")))
          .drop("subjects")
          .withColumn("conf", lit(confidence))
          .withColumn("p", lit(ele(0)))
          .withColumn("o", lit(ele(1)))
          .withColumnRenamed("subject", "s")
          .select("conf", "s", "p", "o"))

    }

  }

}*/