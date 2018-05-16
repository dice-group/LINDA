package org.aksw.dice.linda.classificationRuleMining
import org.apache.spark.sql.{ DataFrame, _ }
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD

object LIBSVMDataConverter {
  val SPACE: String = " ";
  val COLON: String = ":";

  def libsvmwriter(acceptedEntities: DataFrame, nonAcceptedEntities: DataFrame) {
    acceptedEntities.show(false)
    val x = acceptedEntities.rdd.map(r => r.toString())
    x.collect().foreach(println)
  }
}

/* val line: StringBuilder = new StringBuilder()
      line.append("1").append("\t")
      r.getSeq[Double](0).foreach(a => {
        line.append(a + COLON + "1").append(SPACE)
      })
      line.toString()*/