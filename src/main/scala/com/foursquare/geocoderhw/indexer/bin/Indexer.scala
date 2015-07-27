package com.foursquare.geocoderhw.indexer.bin

import scala.io.Source
import org.bson.types.ObjectId
import com.foursquare.geocoderhw.boot.Boot
import com.foursquare.geocoderhw.model.FeatureRecord


/* Chicago        4887398
 * Cook county    4888671   ADMIN2
 * Illinois       4896861   ADMIN1
 * USA            6252001
 * New York City  5128581
 * New York       5128638   ADMIN1
 * grep "4887398\|4888671\|4896861\|6252001\|5128581\|5128638" sources/US.txt  > sources/US_nyc_chi.txt
 */


object GeonamesEntry {
  def fromLine(line: String): Option[GeonamesEntry] = {
    val lineParts = line.split('\t')
    if (lineParts.length > 17) {
      Some(new GeonamesEntry(lineParts))
    } else None
  }

  def fromCountryLine(line: String): Option[GeonamesCountryEntry] = {
    val lineParts = line.split('\t')
    if (lineParts.length == 18) {
      Some(new GeonamesCountryEntry(lineParts))
    } else None
  }

  def fromAdmin1Line(line: String): Option[GeonamesAdmin1Entry] = {
    val lineParts = line.split('\t')
    if (lineParts.length == 4) {
      Some(new GeonamesAdmin1Entry(lineParts))
    } else None
  }

  def fromAdmin2Line(line: String): Option[GeonamesAdmin2Entry] = {
    val lineParts = line.split('\t')
    if (lineParts.length == 4) {
      Some(new GeonamesAdmin2Entry(lineParts))
    } else None
  }

  def fromAlternateNameLine(line: String): Option[GeonamesAlternateNameEntry] = {
    val lineParts = line.split('\t')
    if (lineParts.length >= 4) {
      Some(new GeonamesAlternateNameEntry(lineParts))
    } else None
  }
}

class GeonamesCountryEntry(lineParts: Seq[String]) {
  def stringAt(idx: Int): Option[String] = {
    lineParts.lift(idx).filter(_.nonEmpty)
  }

  def countryCode: Option[String] = stringAt(0)
  def geonameid: Option[Long] = stringAt(16).map(_.toLong)
  override def toString: String = lineParts.mkString("\t")
}


class GeonamesAdmin1Entry(lineParts: Seq[String]) {
  def stringAt(idx: Int): Option[String] = {
    lineParts.lift(idx).filter(_.nonEmpty)
  }

  def countryCode: Option[String] = stringAt(0).flatMap(_.split('.').lift(0))
  def admin1Code: Option[String] = stringAt(0).flatMap(_.split('.').lift(1))
  def name: Option[String] = stringAt(1)
  def geonameid: Option[Long] = stringAt(3).map(_.toLong)
  override def toString: String = lineParts.mkString("\t")
}


class GeonamesAdmin2Entry(lineParts: Seq[String]) {
  def stringAt(idx: Int): Option[String] = {
    lineParts.lift(idx).filter(_.nonEmpty)
  }

  def countryCode: Option[String] = stringAt(0).flatMap(_.split('.').lift(0))
  def admin1Code: Option[String] = stringAt(0).flatMap(_.split('.').lift(1))
  def admin2Code: Option[String] = stringAt(0).flatMap(_.split('.').lift(2))
  def name: Option[String] = stringAt(1)
  def geonameid: Option[Long] = stringAt(3).map(_.toLong)
  override def toString: String = lineParts.mkString("\t")
}

class GeonamesAlternateNameEntry(lineParts: Seq[String]) {
  def stringAt(idx: Int): Option[String] = {
    lineParts.lift(idx).filter(_.nonEmpty)
  }

  def alternateNameId: Option[Long] = stringAt(0).map(_.toLong)
  def geonameid: Option[Long] = stringAt(1).map(_.toLong)
  def isoLanguage: Option[String] = stringAt(2)
  def alternateName: Option[String] = stringAt(3)
  def isPreferredName: Boolean = stringAt(4).exists(_.toInt == 1)
  def isShortName: Boolean = stringAt(5).exists(_.toInt == 1)
  def isColloquial: Boolean = stringAt(6).exists(_.toInt == 1)
  def isHistoric: Boolean = stringAt(7).exists(_.toInt == 1)
  override def toString: String = lineParts.mkString("\t")
}

class GeonamesEntry(lineParts: Seq[String]) {
  def stringAt(idx: Int): Option[String] = {
    lineParts.lift(idx).filter(_.nonEmpty)
  }

  def geonameid: Option[Long] = stringAt(0).map(_.toLong)
  def name: Option[String] = stringAt(1)
  def nameAscii: Option[String] = stringAt(2)
  def alternateNames: Option[Seq[String]] = stringAt(3).map(_.split(','))
  def latitude: Option[Double] = stringAt(4).map(_.toDouble)
  def longitude: Option[Double] = stringAt(5).map(_.toDouble)
  def featureClass: Option[String] = stringAt(6)
  def featureCode: Option[String] = stringAt(7)
  def countryCode: Option[String] = stringAt(8)
  def cc2: Option[String] = stringAt(9)
  def admin1Code: Option[String] = stringAt(10)
  def admin2Code: Option[String] = stringAt(11)
  def admin3Code: Option[String] = stringAt(12)
  def admin4Code: Option[String] = stringAt(13)
  def population: Option[Long] = stringAt(14).map(_.toLong)
  def elevation: Option[Long] = stringAt(15).map(_.toLong)
  def dem: Option[Long] = stringAt(16).map(_.toLong)
  def timezone: Option[String] = stringAt(17)

  def admin1Key: Option[(String, String)] = for {
    country <- countryCode
    admin1 <- admin1Code
  } yield (country, admin1)

  def admin2Key: Option[(String, String, String)] = for {
    (country, admin1) <- admin1Key
    admin2 <- admin2Code
  } yield (country, admin1, admin2)

  override def toString: String = lineParts.mkString("\t")
}

case class GenonamesIndexes(
  countryCodes: Map[String, Long],
  admin1Codes: Map[(String, String), Long],
  admin2Codes: Map[(String, String, String), Long]
)


case class GeocoderEntry(
  id: ObjectId,
  geonameid: Long,
  geonamesEntry: GeonamesEntry,
  membership: Seq[Long],
  names: Seq[String] = Nil,
  preferred: Option[String] = None,
  abbreviated: Option[String] = None
) {
}



object Indexer {
  def readGeonames(
    countriesPath: String,
    admin1Path: String,
    admin2Path: String,
    alternateNamesPath: String,
    entriesPath: String
  ): Map[Long, GeocoderEntry] = {
    val countryCodes = Source.fromFile(countriesPath).getLines()
      .flatMap(GeonamesEntry.fromCountryLine)
      .flatMap(e => for {
        country <- e.countryCode
        geonameid <- e.geonameid
      } yield country -> geonameid)
      .toMap

    val admin1Codes = Source.fromFile(admin1Path).getLines()
      .flatMap(GeonamesEntry.fromAdmin1Line)
      .flatMap(e => for {
        country <- e.countryCode
        admin1 <- e.admin1Code
        geonameid <- e.geonameid
      } yield (country, admin1) -> geonameid)
      .toMap

    val admin2Codes = Source.fromFile(admin2Path).getLines()
      .flatMap(GeonamesEntry.fromAdmin2Line)
      .flatMap(e => for {
        country <- e.countryCode
        admin1 <- e.admin1Code
        admin2 <- e.admin2Code
        geonameid <- e.geonameid
      } yield (country, admin1, admin2) -> geonameid)
      .toMap

    var entries = Source.fromFile(entriesPath).getLines()
      .flatMap(GeonamesEntry.fromLine)
      .filter(_.featureClass.map(_ match {
        case "A" => true
        case "P" => true
        case _ => false
      }).getOrElse(false))
      .flatMap(e => for {
        geonameid <- e.geonameid
      } yield geonameid -> GeocoderEntry(
          new ObjectId(),
          geonameid,
          e,
          (e.countryCode.flatMap(countryCodes.get).toList ++
            e.admin1Key.flatMap(admin1Codes.get) ++
            e.admin2Key.flatMap(admin2Codes.get)
          ).filterNot(_ == geonameid),
          e.name.toList ::: List("geonameid:" + geonameid)
        )
      )
      .toMap

    Source.fromFile(alternateNamesPath).getLines()
      .flatMap(GeonamesEntry.fromAlternateNameLine)
      .foreach(e => for {
        geonameid <- e.geonameid
        entry <- entries.get(geonameid)
        lang <- e.isoLanguage
        if (lang == "en" || lang == "abbr")
        alternateName <- e.alternateName
      } {
        entries += (geonameid -> entry.copy(
          names = alternateName +: entry.names,
          preferred = if (e.isPreferredName) Some(alternateName) else entry.preferred,
          abbreviated = if (lang == "abbr") Some(alternateName) else entry.abbreviated
        ))
      })
    entries
  }

  def saveGeonamesFeatures(entries: Map[Long, GeocoderEntry]) {
    entries.foreach({ case (id, entry) => {
      val rec = FeatureRecord.createRecord
        .id(entry.id)
        .names(entry.names.toList)
        .membership(entry.membership.flatMap(entries.get).map(_.id).distinct.toList)
        .preferred(entry.preferred)
        .abbreviated(entry.abbreviated)
      rec.save(true)
    }})

  }

  def main(args: Array[String]): Unit = {
    Boot.bootMongo(List(FeatureRecord))

    val geonamesEntries = Indexer.readGeonames(
      "sources/countryInfo.txt",
      "sources/admin1CodesASCII.txt",
      "sources/admin2Codes.txt",
      "sources/alternateNames.txt",
      "sources/US_nyc_chi.txt"
    )
    saveGeonamesFeatures(geonamesEntries)

  }
}
