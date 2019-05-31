import scala.util.Try
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpResponse}


def frameHeaders(authToken: String) = {
  Seq(("X-Auth-Token", authToken), ("Content-Type", "application/json"), ("X-App-Token", "SomeToken"), ("X-App-Version", "SomeVersion"))
}



def getResponse(authToken: String, url: String, isScroll: Boolean=true , query: Option[String]= None):
HttpResponse[String] = {
  if (!isScroll) {
    val response: HttpResponse[String] = Try(Http(url).postData(query.get).headers(frameHeaders(authToken)).option(HttpOptions.connTimeout(10000)).option(HttpOptions.readTimeout(60000)).asString).
      getOrElse(Http(url).postData(query.get).headers(frameHeaders(authToken)).option(HttpOptions.connTimeout(10000)).option(HttpOptions.readTimeout(60000)).asString)
    response
  }
  else {
    val scroll_response: HttpResponse[String] = Try(Http(url).headers(frameHeaders(authToken)).option(HttpOptions.connTimeout(10000)).option(HttpOptions.readTimeout(60000)).asString).
      getOrElse(Http(url).headers(frameHeaders(authToken)).option(HttpOptions.connTimeout(10000)).option(HttpOptions.readTimeout(60000)).asString)
    scroll_response
  }
}