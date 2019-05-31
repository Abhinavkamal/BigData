import com.abhinav.sample.codeHacks._
import scala.util.Try


object convertDoubleToInt {

  def convertDoubleToInt(input: String): String = {
    Try(input.toDouble.toInt.toString).getOrElse(Try(input).getOrElse(""))
  }
}