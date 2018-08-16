  package org.aksw.dice.linda.Utils

object LINDAProperties {
  /*
  //"hdfs://172.18.160.17:54310/KunalJha/"
   * "wordnet-mlj12-train"
   *   */

  final val SPARK_SYSTEM = "local[*]"
  //final val SPARK_SYSTEM = "spark://172.18.160.16:3090"

  final val SERIALIZER = "spark.serializer"
  final val KYRO_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  final val APP_EWS_FACTS = "LINDA  (Facts Generator)"
  final val APP_DATASET_PROCESSOR = "LINDA (Dataset Processor)"
  final val APP_EWS_MINER = "LINDA  (EWS Miner)"
  final val APP_DT_MINER = "LINDA (DT Miner)"
  final val APP_DT_FACTS = "LINDA (DT Facts)"

}
