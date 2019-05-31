import play.api.libs.json.{JsValue, Json}


var input: JsValue = Json.parse("someInput")

def extractFromJSValue(input: JsValue, extractValue: String, extractType: String): String = {
  val emptyString = ""
  extractType match {
    case "str" => (input \ extractValue).asOpt[String].getOrElse(emptyString)
    case "list" => (input \ extractValue).asOpt[List[String]].getOrElse(Nil).headOption.getOrElse(emptyString)
    case "listr" => (input \\ extractValue).headOption.map(_.asOpt[String].getOrElse(emptyString)).getOrElse(emptyString)
  }
}

def extractInfoFromJSValue(input: JsValue, extractValue: String): String = {
  val emptyString = ""
  input.\\(extractValue).headOption.map(x => x.as[String]).getOrElse(emptyString)
}