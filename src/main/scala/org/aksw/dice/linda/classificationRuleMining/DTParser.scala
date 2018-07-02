package org.aksw.dice.linda.classificationRuleMining

import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import scala.collection.mutable.ListBuffer
import com.google.gson.Gson
import util.control.Breaks._

object DTParser {
  case class Rule(antecedant: List[Int], consequent: Int, negation: List[Int]) {
    override def toString() = this.antecedant + " " + this.negation + " " + this.consequent
  }
  def parse(tree: DecisionTreeClassificationModel, id: Int): List[Rule] = {
    var lines = tree.toDebugString.lines.toList
    var stack = new ListBuffer[String]()
    var result = new ListBuffer[List[String]]()
    while (lines.length > 0) {
      if (lines(0).contains("If")) {
        stack.append(lines(0).trim())
      } else if (lines(0).contains("Predict")) {
        result.append(stack.toList :+ lines(0).replace(")", "").trim())
        if (stack(stack.length - 1).contains("Else"))
          stack.remove(stack.length - 1)
      } else if (lines(0).contains("Else")) {
        stack.remove(stack.length - 1)
        stack.append(lines(0).trim())
      }
      lines = lines.tail
    }
    result.toList.filter(r => !r(r.size - 1).contains("Predict: 0.0")).map(r =>
      parserLine(r, id))
  }
  def parserLine(line: List[String], id: Int): Rule = {
    var antecedant = new ListBuffer[Int]
    var negation = new ListBuffer[Int]
    line.foreach(a => {
      if (!a.contains("Predict")) {
        var content = a.substring(a.indexOf('('), a.indexOf(')')).split(" ")
        if (((content.contains("not")) && (content.contains("{0.0}")))
          || (((!content.contains("not")) && (content.contains("{1.0}"))))) {
          antecedant.append(content(1).toInt)
        } else if ((!content.contains("not")) && (content.contains("{0.0}"))
          || (((content.contains("not")) && (content.contains("{1.0}"))))) {
          negation.append(content(1).toInt)
        }
      }
    })
    //Cannot do transformation here as it is in memory
    new Rule(antecedant.toList, id, negation.toList)
  }

}