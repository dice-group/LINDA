package org.aksw.dice.linda.classificationRuleMining
import org.apache.spark.sql.{ DataFrame, _ }
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object LIBSVMConverter {

  val SPACE: String = " ";
  val COLON: String = ":";

  def libsvmwriter(acceptedEntities: DataFrame, nonAcceptedEntities: DataFrame) {
    val a = acceptedEntities.rdd.map(r => {
      val line: StringBuilder = new StringBuilder()
      line.append("1").append("\t")
      r.getSeq[Double](0).foreach(a => {
        line.append(a + COLON + "1").append(SPACE)
      })
      line.toString()
    })

  }
}

/* */
