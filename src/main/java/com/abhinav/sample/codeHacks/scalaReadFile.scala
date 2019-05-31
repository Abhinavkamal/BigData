import java.io.InputStream
import scala.io.Source

def readResourceFile(path: String): String = {

  val stream: InputStream = getClass.getResourceAsStream(path)
  val lines = scala.io.Source.fromInputStream(stream)
  try {
    lines.getLines().mkString
  }
  finally{
    lines.close()
  }
}

def readFile(path: String): List[String] = {
  val text = Source.fromFile(path)
  text.getLines().toList
}