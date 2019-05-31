import org.apache.hadoop.conf._
import org.apache.spark.sql.functions.col
import java.io.{BufferedInputStream, OutputStreamWriter}
import scala.io.Source
import scala.util.parsing.json.JSON
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer

object ReadWriteHdfs {

  implicit val spark: SparkSession = SparkSession.builder().getOrCreate()
  implicit val sc: SparkContext = spark.sparkContext
  val hadoopConf: Configuration = sc.hadoopConfiguration
  implicit val hadoop: FileSystem = FileSystem.get(sc.hadoopConfiguration)


  def readAndMap( path : String, mapper : ( String ) => Unit ) = {
    if ( hadoop.exists( new Path( path ) ) ) {
      val is = new BufferedInputStream( hadoop.open( new Path( path ) ) )
      Source.fromInputStream( is ).getLines( ).foreach( mapper )
    }
    else {
      // TODO - error logic here
    }
  }

  def write( filename : String, content : Iterator[ String ] ) = {
    val path = new Path( filename )
    val out = new OutputStreamWriter( hadoop.create( path, false ) )
    content.foreach( str => out.write( str + "\n" ) )
    out.flush( )
    out.close( )
  }

  def ls( path : String ) : List[ String ] = {
    val files = hadoop.listFiles( new Path( path ), false )
    val filenames = ListBuffer[ String ]( )
    while ( files.hasNext ) filenames += files.next( ).getPath( ).toString( )
    filenames.toList
  }

  def rm( path : String, recursive : Boolean ) : Unit = {
    if ( hadoop.exists( new Path( path ) ) ) {
      println( "deleting file : " + path )
      hadoop.delete( new Path( path ), recursive )
    }
    else {
      println( "File/Directory" + path + " does not exist" )
    }
  }

  def cat( path : String ) = Source.fromInputStream( hadoop.open( new Path( path ) ) ).getLines( ).foreach( println )

  def dirExists(hdfsDirectory: String): Boolean =
  {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(hdfsDirectory))
    exists
  }

}