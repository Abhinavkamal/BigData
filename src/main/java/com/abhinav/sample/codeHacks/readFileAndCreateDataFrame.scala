import org.apache.spark.sql.{DataFrame, SparkSession}

def readTextFile(path: String)(implicit spark: SparkSession): DataFrame = {
  spark.read.textFile(path).toDF()
}


def readCsvFile(path: String)(implicit spark: SparkSession): DataFrame = {
  spark.read.format(source = "csv").option("header", "true").load(path)
}

