import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

import scala.util.Try

object copyHdfsFilesbetweenTwoPaths{

  val spark = SparkSession.builder().getOrCreate()
  val sc = spark.sparkContext
  val hadoopConf = sc.hadoopConfiguration

def copyFilesToNewPath(basePath: String, filePath: String, newPath: String): Unit = {
  val absolutePath = basePath + filePath
  val newAbsolutePath = newPath + filePath
  val fs = FileSystem.get(hadoopConf)
  val listOFilesToLoad = fs.listStatus(new Path(absolutePath)).
    map(x => x.getPath.toString).
    filter(_.contains("part")).toList
  Try(fs.delete(new Path(newAbsolutePath), true))
  Try(fs.mkdirs(new Path(newAbsolutePath)))
  listOFilesToLoad.foreach(file => FileUtil.copy(fs, new Path(file), fs, new Path(newAbsolutePath),
    false, hadoopConf))
}

  def mergeSmallFilesInHdfs(fullPath: String, dirName: String = ""): Unit = {
    val absolutePath = fullPath + dirName
    val new_file_path = absolutePath + "_optimized/part-00000"
    val new_full_path = absolutePath + "_optimized"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    try {
      val bool: Boolean = FileUtil.copyMerge(fs, new Path(absolutePath), fs, new Path(new_file_path), true,
        sc.hadoopConfiguration, "")
      if (bool) fs.rename(new Path(new_full_path), new Path(absolutePath))
    }
    catch {
      case e: Exception => println("Unable to merge files for path", absolutePath)
    }
  }

}
