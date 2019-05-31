import scala.util.parsing.json.JSON



val input = """{"somekey": "someValue"}"""


/** Output Path of todos extraction from source systems on sg33-basemap01-prod
  * @param input Input String
  * @return The long value
  */
def parseStringToLong(input: String) : String = {
  val respJsonParsed = JSON.parseFull(input)
  if (respJsonParsed.isDefined) {
    val respJson = respJsonParsed.get.asInstanceOf[Map[String, Any]]
    val value = java.math.BigInteger.valueOf(respJson.get("item1").map(x => x.asInstanceOf[Map[String, Any]].get("item2").
      map(x => x.asInstanceOf[Map[String, Any]].getOrElse("item3", 0.0)
      ).getOrElse(0.0)).getOrElse(0.0).asInstanceOf[Double].toLong).toString
    value
  }

  else {""}
}