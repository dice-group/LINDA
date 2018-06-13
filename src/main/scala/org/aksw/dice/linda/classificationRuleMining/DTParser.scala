package org.aksw.dice.linda.classificationRuleMining

import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import scala.collection.mutable.ListBuffer
import com.google.gson.Gson
import util.control.Breaks._

object DTParser {
  class Rule(antecedant: String, consequent: String, negation: String) {
    def this(bar: String) = this(bar, null, null)
  }

  def parse(tree: DecisionTreeClassificationModel, id:String) {
    var lines = tree.toDebugString.lines.toList
    var stack = new ListBuffer[String]()
    while (lines.length > 0) {
      if (lines(0).contains("If")) {
        stack.append(lines(0))
      } else if (lines(0).contains("Predict")) {
        println(stack.toString() ++ lines(0))
        if (stack(stack.length - 1).contains("Else"))
          stack.remove(stack.length - 1)

      } else if (lines(0).contains("Else")) {
        stack.remove(stack.length - 1)
        stack.append(lines(0))

      }
      lines = lines.tail
    }
  }
}