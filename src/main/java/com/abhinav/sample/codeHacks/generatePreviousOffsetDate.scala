import java.text.SimpleDateFormat
import java.util.Calendar


def generatePreviousDays(dt : String, offsetValue : Int) : String =
{
  val dateAux = Calendar.getInstance()
  dateAux.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(dt))
  dateAux.add(Calendar.DATE, -offsetValue)
  new SimpleDateFormat("yyyy-MM-dd").format(dateAux.getTime())
}