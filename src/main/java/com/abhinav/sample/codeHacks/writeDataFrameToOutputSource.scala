import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


def writeDataFrameToHive(outputDF: DataFrame, createTable: Boolean, format: String, saveMode: SaveMode, database: String, table: String) = {
  createTable match {
    case true => outputDF.write.format(format).saveAsTable(s"$database.$table")
    case false => outputDF.write.format(format).mode(saveMode).insertInto(s"${database}.${table}")
  }
}


def writeDfToCsv(df: org.apache.spark.sql.DataFrame, csvPath: String)(implicit fs: org.apache.hadoop.fs.FileSystem): Unit = {

  val tmpInputDir = csvPath + "-temp-folder"
  df.coalesce(1).write.option("header","true").format("csv").save(tmpInputDir)
  val file = fs.globStatus(new Path(s"$tmpInputDir/part*"))(0).getPath.getName
  fs.rename(new Path(s"$tmpInputDir/" + file), new Path(csvPath))
  fs.delete(new Path(tmpInputDir),true)
}


def saveToSingleFile[T](rdd: RDD[T], stringPath: String)(implicit spark: SparkSession, fs: FileSystem): Unit = {
  val tempStringPath = stringPath + "_temp_folder"
  rdd.repartition(1).saveAsTextFile(tempStringPath)
  val filePath = new Path(stringPath)
  val oldFilePath = new Path(stringPath + "_old")
  if (fs.exists(filePath)){
    if (fs.exists(oldFilePath)){
      fs.delete(oldFilePath,true)
    }
    fs.rename(filePath, oldFilePath)
  }
  val file = fs.globStatus(new Path(s"$tempStringPath/part*"))(0).getPath.getName
  fs.rename(new Path(s"$tempStringPath/" + file), filePath)
  fs.delete(new Path(tempStringPath),true)
}