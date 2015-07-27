package com.foursquare.geocoderhw.boot

import net.liftweb.json._
import java.util.concurrent.Executors
import net.liftweb.mongodb.{DefaultMongoIdentifier, MongoDB}
import net.liftweb.mongodb.record.{MongoRecord, MongoMetaRecord}
import com.mongodb.{MongoClient, DBAddress, MongoException, MongoClientOptions, ServerAddress}
import com.foursquare.index.{Asc, IndexedRecord}
import scalaj.collection.Imports._

object Boot {
  val DefaultDbHost = "localhost:27017"
  val DefaultDbName = "test"

  def bootMongo(
    indexesToEnsure: List[MongoMetaRecord[_] with IndexedRecord[_]] = Nil
  ) {
    // Mongo
    val dbServerConfig = Option(System.getProperty("db.host")).getOrElse(DefaultDbHost)
    val dbServers = dbServerConfig.split(",").toList.map(a => a.split(":") match {
      case Array(h,p) => new ServerAddress(h, p.toInt)
      case _ => throw new Exception("didn't understand host " + a)
    })
    val mongoOptions = MongoClientOptions.builder
      .socketTimeout(10 * 1000)
      .build
    try {
      val mongo = new MongoClient(dbServers.asJava, mongoOptions)
      val dbname = Option(System.getProperty("db.name")).getOrElse(DefaultDbName)
      MongoDB.defineDb(DefaultMongoIdentifier, mongo, dbname)
      indexesToEnsure.foreach(metaRecord =>
        metaRecord.mongoIndexList.foreach(i =>
          metaRecord.ensureIndex(JObject(i.asListMap.map(fld => JField(fld._1, JInt(fld._2.toString.toInt))).toList)))
      )
    } catch {
      case e: MongoException =>
        throw e
    }
  }
}
