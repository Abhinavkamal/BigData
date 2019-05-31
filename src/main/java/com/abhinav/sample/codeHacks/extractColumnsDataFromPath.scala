import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

def extractColumnsDataFromPath(fullPath: String, filePath: String, tableName: String, database : String,
                             inputColumnArray: Array[String], returnDataFrame: Boolean = true,
                             includePipeline: Boolean = false): org.apache.spark.sql.DataFrame =
{
  val filePathSplitToArray: Array[String] = filePath.toString.split('/')
  var columnsMapped = Map[String, String]()
  for (element <- 1 until inputColumnArray.length) columnsMapped += (inputColumnArray(element) -> filePathSplitToArray(element))
  var columnPartition = ""
  columnsMapped.foreach(key => columnPartition += key._1 + "='" + key._2 + "',")
  columnPartition = columnPartition.substring(0, columnPartition.length - 1)
  val columnsForWhereClause = columnPartition.replaceAll(",", " and ")
  try {
    spark.sql(s"alter table $database.$tableName drop partition($columnPartition)")
  }
  catch {
    case _: Throwable => println("partition does not exists previously")
  }
  if (returnDataFrame) {
    spark.sql(s"alter table $database.$tableName add partition($columnPartition) location '$fullPath'")
    spark.sql(s"select * from  $database.$tableName where $columnsForWhereClause")
  }
  else {
    spark.sql(s"alter table $database.$tableName add partition($columnPartition) location '$fullPath'")
  }
}
