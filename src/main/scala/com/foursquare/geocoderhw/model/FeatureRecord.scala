package com.foursquare.geocoderhw.model

import net.liftweb.mongodb.record.{MongoRecord, MongoMetaRecord}
import net.liftweb.mongodb.record.field.{BsonRecordListField, MongoCaseClassField, MongoListField, ObjectIdPk}
import net.liftweb.record.field._
import net.liftweb.json._
import org.bson.types.ObjectId
import org.joda.time.{DateTime, DateTimeZone}
import com.foursquare.rogue._
import com.foursquare.index.{Asc, IndexedRecord}
import com.foursquare.rogue.lift.LiftRogue._
import java.util.Date


class FeatureRecord extends MongoRecord[FeatureRecord] with ObjectIdPk[FeatureRecord] {
  def meta = FeatureRecord

  object lat extends DoubleField(this)

  object lng extends DoubleField(this)

  object featureClass extends StringField(this, 1) {
    override def name = "fcl"
  }

  object featureCode extends StringField(this, 10) {
    override def name = "fco"
  }

  object abbreviated extends StringField(this, 255) {
    override def name = "a"
  }

  object preferred extends StringField(this, 255) {
    override def name = "p"
  }

  object membership extends MongoListField[FeatureRecord, ObjectId](this) {
    override def name = "m"
  }

  object names extends MongoListField[FeatureRecord, String](this) {
    override def name = "n"
  }

  object s2Cover extends MongoListField[FeatureRecord, Long](this) {
    override def name = "s2c"
  }
}

object FeatureRecord extends FeatureRecord with MongoMetaRecord[FeatureRecord] with IndexedRecord[FeatureRecord] {
  override def collectionName = "features"

  override val mongoIndexList = List(
    FeatureRecord.index(_.id, Asc),
    FeatureRecord.index(_.membership, Asc),
    FeatureRecord.index(_.names, Asc),
    FeatureRecord.index(_.s2Cover, Asc)
  )
}

