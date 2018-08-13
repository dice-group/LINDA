package org.aksw.dice.linda.Utils

object LINDAProperties {
  /*
  //"hdfs://172.18.160.17:54310/KunalJha/"
   * "wordnet-mlj12-train"
   *   */

  final val SPARK_SYSTEM = "local[*]"
  //final val SPARK_SYSTEM = "spark://172.18.160.16:3090"
  var DATASET_NAME: String = _
  var HDFS_MASTER: String = _
  var INPUT_DATASET: String = _

  /*
   * CLUSTER SETTINGS
   * */

  //Dataset Settings
  final val INPUT_DATASET_SUBJECT_OPERATOR_MAP = HDFS_MASTER + DATASET_NAME + "/Maps/SubjectOperatorMap/"
  final val INPUT_DATASET_OPERATOR_SUBJECT_MAP = HDFS_MASTER + DATASET_NAME + "/Maps/OperatorSubjectMap/"
  final val HORN_RULES = HDFS_MASTER + DATASET_NAME + "/Rules/"

  /*
   * EWS Config
   *
   */

  final val EWS_FACTS_WITH_RULES = HDFS_MASTER + "EWS/" + DATASET_NAME + "/Maps/EWSfactswithRules/"
  final val EWS_RULES_JSON = HDFS_MASTER + "EWS/" + DATASET_NAME + "/Rules/"
  final val FACTS_KB_EWS = HDFS_MASTER + "EWS/" + DATASET_NAME + "/Facts/"

  /*
   * DT Config
   *
   */

  final val DT_RULES_JSON = HDFS_MASTER + "DT/" + DATASET_NAME + "/Rules/Json"
  final val FACTS_KB_DT = HDFS_MASTER + "DT/" + DATASET_NAME + "/Facts"
  final val DT_INPUT_DATASET = HDFS_MASTER + "DTAlgo/FinalData/"

  /*
   * SPARK CONFIGS
   *
   */

  final val SERIALIZER = "spark.serializer"
  final val KYRO_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  final val APP_EWS_FACTS = "LINDA  (Facts Generator)"
  final val APP_DATASET_PROCESSOR = "LINDA (Dataset Processor)"
  final val APP_EWS_MINER = "LINDA  (EWS Miner)"
  final val APP_DT_MINER = "LINDA (DT Miner)"

}
