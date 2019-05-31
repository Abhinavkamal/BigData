import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.joda.time.DateTimeZone

def getStartEndTimestamp(inputDate: String, timeZone : String = "UTC") : Map[String, String] = {

  DateTimeZone.setDefault(DateTimeZone.forID(timeZone))
  TimeZone.setDefault(TimeZone.getTimeZone(timeZone))

  val DATE_FORMAT = "yyyy-MM-dd"
  val today = new SimpleDateFormat(DATE_FORMAT).parse(inputDate)
  val nextDay = new Date(today.getTime() + (1000 * 60 * 60 * 24))

  Map("greaterThanTime" -> today.toInstant.toEpochMilli().toString, "lessThanTime" -> nextDay.toInstant.toEpochMilli().toString)

}
