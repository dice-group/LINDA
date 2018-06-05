package org.aksw.dice.linda.Factgeneration

import org.apache.spark.sql.SparkSession

object FactGenerator {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("LINDA  (Original Miner)")
      .getOrCreate()
    val context = spark.sparkContext
    val data = spark.read.format("libsvm").load("/Users/Kunal/workspaceThesis/LINDA/Data/LIBSVMData/0/00")
  }
}