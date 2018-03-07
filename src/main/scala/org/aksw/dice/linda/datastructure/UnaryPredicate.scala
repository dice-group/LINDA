package org.aksw.dice.linda.datastructure

class UnaryPredicate {
  var predicate: String = _
  var obj: String = _
  var id: Double = _

  def this(id: Double, pred: String, ob: String) {
    this()
    this.predicate = pred
    this.id = id
    this.obj = ob
  }

  def this(itemString: String) {
    this()
    var item = itemString.split(",")
    this.predicate = item(0)
    this.id = -1
    this.obj = item(1)
  }

 

  override def toString(): String = {
    "(" + this.predicate + "," + this.obj + ")"
  }
}
