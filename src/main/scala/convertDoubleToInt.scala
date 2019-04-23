import scala.util.Try

def convertDoubleToInt(input: String): String = {
  Try(input.toDouble.toInt.toString).getOrElse(Try(input).getOrElse(""))
}