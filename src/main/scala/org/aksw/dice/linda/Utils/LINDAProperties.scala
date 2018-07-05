package org.aksw.dice.linda.Utils

object LINDAProperties {
  /*
   * DATASET CONFIGS
   */
  //TODO: Add Other datasets
  final val DATASET_NAME = "rdf"
  final val INPUT_DATASET = "Data/rdf.nt"
  final val INPUT_DATASET_SUBJECT_OPERATOR_MAP = "Data/" + DATASET_NAME + "/Maps/SubjectOperatorMap/parquet"

  final val OPERATORID_MAP = "Data/" + DATASET_NAME + "/Maps/OperatorId/parquet"
  final val OPERATORID_MAP_JSON = "Data/" + DATASET_NAME + "/Maps/OperatorId/json"
  final val INPUT_DATASET_OPERATOR_SUBJECT_MAP = "Data/" + DATASET_NAME + "/Maps/OperatorSubjectMap/parquet"
  final val INPUT_DATASET_OPERATOR_SUBJECT_JSON = "Data/" + DATASET_NAME + "/Maps/OperatorSubjectMap/parquet"

  /*
   * LINDA CONFIGS
   */
  final val EWS_RULES = "Data/EWSAlgo/" + DATASET_NAME + "/Rules/Parquet"
  final val EWS_RULES_JSON = "Data/EWSAlgo/" + DATASET_NAME + "/Rules/Json"
  final val DT_RULES = "Data/DTAlgo/" + DATASET_NAME + "/Rules/Parquet"
  final val DT_RULES_JSON = "Data/DTAlgo/" + DATASET_NAME + "/Rules/Json"
  final val RESULT_KB_EWS = "Data/EWSAlgo/" + DATASET_NAME + "/New Dataset/"
  final val RESULT_KB_DT = "Data/DTAlgo/" + DATASET_NAME + "/New Dataset/"
  final val LIBSVM_DATASET = "Data/DTAlgo/LIBSVMData/" + DATASET_NAME + "/"

  /*
   * SPARK CONFIGS
   */
  final val SPARK_SYSTEM = "local[*]"
  final val SERIALIZER = "spark.serializer"
  final val KYRO_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  final val WAREHOUSE = "spark.sql.warehouse.dir"
  final val DIRECTORY = "Users/Kunal/workspaceThesis/LINDA/"
  final val APP_DATASET_CREATER = "LINDA (Data Set Creater)"
  final val APP_FACT_GENERATOR = "LINDA  (Fact Generator)"
  final val APP_EWS_MINER = "LINDA  (EWS Miner)"
  final val APP_DT_MINER = "LINDA (DT Miner)"

}