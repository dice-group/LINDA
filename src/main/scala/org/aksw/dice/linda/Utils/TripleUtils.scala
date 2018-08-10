package org.aksw.dice.linda.Utils

object TripleUtils {

  def parsTriples(parsData: String): Triples = {
    val subRAngle = parsData.indexOf('>')
    val predLAngle = parsData.indexOf('<', subRAngle + 1)
    val predRAngle = parsData.indexOf('>', predLAngle + 1)
    var objLAngle = parsData.indexOf('<', predRAngle + 1)
    var objRAngle = parsData.indexOf('>', objLAngle + 1)

    if (objRAngle == -1) {
      objLAngle = parsData.indexOf('\"', objRAngle + 1)
      objRAngle = parsData.indexOf('\"', objLAngle + 1)
    }

    val subject = parsData.substring(0, subRAngle +1)
    val predicate = parsData.substring(predLAngle, predRAngle + 1)
    val `object` = parsData.substring(objLAngle + 1, objRAngle + 1)

    Triples(subject, predicate, `object`)
  }

}
case class Triples(subject: String, predicate: String, `object`: String) {
  def isLangTag(resource: String) = resource.startsWith("@")
}