import java.text.SimpleDateFormat

def epochToDate(epochMillis: Long): String = {
  val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  df.format(epochMillis)
}