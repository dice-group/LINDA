package org.aksw.dice.linda.Utils

object LINDAProperties {
  /*
   * DATASET CONFIGS
   */
  //
  //TODO: Add Other datasets
  final val DATASET_NAME = "freebase_mtr100_mte100-train"
  final val HDFS_MASTER = "hdfs://172.18.160.17:54310/kunal/"

  final val INPUT_DATASET = HDFS_MASTER + DATASET_NAME + ".nt"

  final val INPUT_DATASET_SUBJECT_OPERATOR_MAP = "Data/" + DATASET_NAME + "/Maps/SubjectOperatorMap/"
  final val OPERATOR_ID_MAP = "Data/" + DATASET_NAME + "/Maps/OperatorId/parquet"
  final val INPUT_DATASET_OPERATOR_SUBJECT_MAP = "Data/" + DATASET_NAME + "/Maps/OperatorSubjectMap/parquet"

  /*
   * LINDA CONFIGS
   */
  final val EWS_RULES_JSON = "Data/" + "EWS/" + DATASET_NAME + "/Rules/Json"
  final val EWS_RULES_PARQUET = "Data/" + "EWS/" + DATASET_NAME + "/Rules/Parquet"
  final val FACTS_KB_EWS = "Data/" + "EWS/" + DATASET_NAME + "/New Facts/EWSresult.csv"
  final val DT_RULES_JSON = "Data/" + "DT/" + DATASET_NAME + "/Rules/Json"
  final val DT_RULES_PARQUET = "Data/" + "DT/" + DATASET_NAME + "/Rules/Parquet"
  final val FACTS_KB_DT = "Data/" + "DT/" + DATASET_NAME + "/New Facts/DTresult.csv"
  final val LIBSVM_DATASET = "Data/" + "DTAlgo/LIBSVMData/"
  final val DT_INPUT_DATASET = "Data/" + "DTAlgo/FinalData/"

  /*
   * SPARK CONFIGS
   */
  //final val SPARK_SYSTEM_LOCAL = "local[*]"
  final val SPARK_SYSTEM = "spark://172.18.160.16:3090"
  final val SERIALIZER = "spark.serializer"
  final val KYRO_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  final val WAREHOUSE = "spark.sql.warehouse.dir"

  final val APP_DATASET_CREATER = "LINDA (Data Set Creater)"
  final val APP_EWS_MINER = "LINDA  (EWS Miner)"
  final val APP_DT_MINER = "LINDA (DT Miner)"

}