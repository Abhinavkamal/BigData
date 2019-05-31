import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit


def checkColumnExists(inputDF: DataFrame, checkColList: List[String]): DataFrame = {
  var outputDF = inputDF
  val dFCols = inputDF.columns.toList
  checkColList.foreach(col => if (!dFCols.contains(col)) outputDF = outputDF.withColumn(col, lit("")))
  outputDF
}
