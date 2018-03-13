package org.aksw.dice.linda.datastructure

class Rules {
  var head: UnaryPredicate = null
  var body: List[UnaryPredicate] = List()

  var confidence: Int = _

  override def toString(): String ={
      return new String("\t" + head.toString() + "\t[" + body.mkString(" ") + "]")
    }

}