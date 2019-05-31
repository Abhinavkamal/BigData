import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.split

def sparkSplitColumnValueToMultipleColumns(inputDataFrame: DataFrame,columnToSplit: String, ExplodeToMultipleColumns: List[String]): DataFrame =
{
  var outputDataFrame = inputDataFrame
  for (col <- ExplodeToMultipleColumns.indices)
  {
    outputDataFrame = outputDataFrame.withColumn(ExplodeToMultipleColumns(col), split(outputDataFrame(columnToSplit), "\t").getItem(col))
  }
  outputDataFrame
}

def renameDataFrameColumns(inputDataFrame: DataFrame, InputColumnList : List[String], renameString : String) : DataFrame =
{
  var OutputDataFrame = inputDataFrame
  InputColumnList.foreach(column => { OutputDataFrame = OutputDataFrame.withColumnRenamed(column, column+renameString) })
  OutputDataFrame
}