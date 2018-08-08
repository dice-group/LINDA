package org.aksw.dice.linda.Utils

object LINDAProperties {
  /*
  * DATASET CONFIGS
  */
  //
  //TODO: Add Other datasets
  final val DATASET_NAME = "imdb"
  final val HDFS_MASTER = "hdfs://172.18.160.17:54310/KunalJha/"

  final val INPUT_DATASET = HDFS_MASTER + DATASET_NAME + ".nt"

  final val INPUT_DATASET_SUBJECT_OPERATOR_MAP = HDFS_MASTER + DATASET_NAME + "/Maps/SubjectOperatorMap/"
  final val OPERATOR_ID_MAP = HDFS_MASTER + DATASET_NAME + "/Maps/OperatorId/parquet"
  final val INPUT_DATASET_OPERATOR_SUBJECT_MAP = HDFS_MASTER + DATASET_NAME + "/Maps/OperatorSubjectMap/parquet"

  /*
   * LINDA CONFIGS
   */
  final val EWS_RULES_JSON = HDFS_MASTER + "EWS/" + DATASET_NAME + "/Rules/ewsrules"
  final val EWS_RULES_PARQUET = HDFS_MASTER + "EWS/" + DATASET_NAME + "/Rules/Parquet"
  final val FACTS_KB_EWS = HDFS_MASTER + "EWS/" + DATASET_NAME + "/Facts"
  final val DT_RULES_JSON = HDFS_MASTER + "DT/" + DATASET_NAME + "/Rules/Json"
  final val DT_RULES_PARQUET = HDFS_MASTER + "DT/" + DATASET_NAME + "/Rules/Parquet"
  final val FACTS_KB_DT = HDFS_MASTER + "DT/" + DATASET_NAME + "/Facts"
  final val LIBSVM_DATASET = HDFS_MASTER + "DTAlgo/" + DATASET_NAME + "/LIBSVMData/"
  final val DT_INPUT_DATASET = HDFS_MASTER + "DTAlgo/FinalData/"

  /*
   * SPARK CONFIGS
   *
   */

  final val SPARK_SYSTEM = "local[*]"
  //final val SPARK_SYSTEM = "spark://172.18.160.16:3090"
  final val SERIALIZER = "spark.serializer"
  final val KYRO_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  final val WAREHOUSE = "spark.sql.warehouse.dir"
  final val APP_DATASET_CREATER = "LINDA (Data Set Creater)"
  final val APP_EWS_MINER = "LINDA  (EWS Miner)"
  final val APP_DT_MINER = "LINDA (DT Miner)"

}
