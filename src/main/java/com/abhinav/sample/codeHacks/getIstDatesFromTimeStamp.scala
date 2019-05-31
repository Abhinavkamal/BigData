
def getIstDateFromTimestamp(timestamp : Long) : String =
{
  val date = new java.util.Date(timestamp)
  val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
  sdf.setTimeZone(java.util.TimeZone.getTimeZone("IST"))
  sdf.format(date)
}