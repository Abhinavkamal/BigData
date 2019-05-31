
package com.abhinav.sample.codeHacks
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mongodb.scala.{Completed, Document, MongoClient, MongoCollection, MongoDatabase, Observable, Observer}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model._
import play.api.libs.json.{JsArray, JsObject, JsValue, Json}
import com.mongodb.client.model.Projections
import org.bson._
import java.util.Date
import java.util.concurrent.TimeUnit\
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Try



object HiveToMongoDB  extends  App   {

  implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: (Document) => String = (doc) => doc.toJson
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: (C) => String = (doc) => doc.toString
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: (C) => String

    def results(): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))
    def headResult() = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))
    def printResults(initial: String = ""): Unit = {
      if (initial.length > 0) print(initial)
      results().foreach(res => println(converter(res)))
    }
    def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
  }

  val spark: SparkSession = SparkSession.builder().getOrCreate()

  val mongoUrl = "mongoUrl"
  val mongoDatabase = "mongoDatabase"
  val mongoCollection = "mongoCollection"
  val sourceTables = "sourceTables"
  val sourceDatabase = "sourceDatabase"
  val userName = "userName"


  val client = MongoClient(mongoUrl)


  val getmongodatabase: MongoDatabase = client.getDatabase(mongoDatabase)


  val collection: MongoCollection[Document] = getmongodatabase.getCollection(mongoCollection)


  def findAndUpdateMongoData(inputDataframe : DataFrame) :Unit = {
    var lis = inputDataframe.toJSON.collect().toSeq.map(x => Document(x))
    lis.map { details =>

      val col5Records: Seq[JsValue] = collection.find(and(equal("col1", details("col1")),
        equal("col2", details("col2"))))
        .projection(Projections.fields(Projections.include("col3"),
          Projections.excludeId())).results()
        .map(item => Json.parse(item.toJson()).as[JsValue])

      val col5Array: JsArray = col5Records.headOption.fold(JsArray()) { record =>
        (record \ "col3").as[List[JsValue]].foldLeft(JsArray()) { (currentDocument, json) =>
          currentDocument :+ json
        }
      }

      val username = Json.parse(s"""{"created_by": "$userName"}""")

      val updatedcol5Value = Try(details("col5").asInt32().getValue).getOrElse(details("col5").asString().getValue.toInt)

      val newcol5s =   Json.obj("created_on" -> Json.obj("$date" -> new Date())) ++
        username.as[JsObject] ++ Json.obj("value" -> updatedcol5Value)

      val updatedArray: JsValue =  col5Array :+ newcol5s

      collection.findOneAndUpdate(and(equal("col1", details("col1")),
        equal("col2", details("col2")),
        equal("subType", details("subType"))),
        combine(set("col4", updatedcol5Value),
          set("col5s",
            BsonArray.parse(Json.stringify(updatedArray)))),
        new FindOneAndUpdateOptions().upsert(true)
      ).results()
    }
  }

      var data = spark.read.option("header","true").csv("someFile.txt")
      findAndUpdateMongoData(data)


}