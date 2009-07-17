/**
 * Created by IntelliJ IDEA.
 * User: alexyk
 * Date: Jul 16, 2009
 * Time: 2:05:57 PM
 */

package com.tfitter.json

import System.err
import org.codehaus.jackson._
import org.apache.commons.lang.StringEscapeUtils.unescapeHtml
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import db.{User,Twit,ReplyTwit,UserTwit}
import db.types._


object TwitExtract {
  val factory: JsonFactory = new JsonFactory
// for extracting the time
  val dateTimeFmt = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z YYYY")

  def parse(s:String): UserTwit = try {
              // Twitter forgot to scrub ^? in some user's location/description,
              // see status 2174151307
              // entering chars by code, equivalent methods:
              // "\u007f"
              // 127.toChar.toString{
    val line = s.replaceAll("\u007f", "")
    // upon insertion into PG, we get some exceptions like,
    // ERROR: invalid byte sequence for encoding "UTF8": 0x00 tid=2174544806
    // but removing \u0000 doesn't fix it; should we try 0x00 directly?
    // val line = s1.replaceAll("\u0000", "")
    val jp: JsonParser = factory.createJsonParser(line);

    // user
    var _uid: Option[String] = None
    var name: Option[String] = None // optional
    var _screenName: Option[String] = None
    var _statusesCount: Option[String] = None
    var _friendsCount: Option[String] = None
    var _userCreatedAt: Option[String] = None
    var location: Option[String] = None // optional
    // should we also capture/decode time_zone?
    var _utcOffset: Option[String] = None
    // twit
    var _tid: Option[String] = None
    var _twitCreatedAt: Option[String] = None
    var _twitText: Option[String] = None
    var _replyTwit: Option[String] = None
    var _replyUser: Option[String] = None

    jp.nextToken
    while (jp.nextToken != JsonToken.END_OBJECT) {
      val fieldName = jp.getCurrentName
      jp.nextToken
      fieldName match {
        case "user" =>
          while (jp.nextToken != JsonToken.END_OBJECT) {
            val nameField = jp.getCurrentName
            jp.nextToken
             nameField match {
              case "id" =>
                _uid = Opt(jp.getText)
              case "name" =>
                name = Opt(jp.getText)
              case "screen_name" =>
                _screenName = Opt(jp.getText)
              case "statuses_count" =>
                _statusesCount = Opt(jp.getText)
              case "friends_count" =>
                _friendsCount = Opt(jp.getText)
              case "location" =>
                location = Opt(jp.getText)
              case "utc_offset" =>
                _utcOffset = Opt(jp.getText)
              case "created_at" =>
                _userCreatedAt = Opt(jp.getText)
              case _ => ()
            }
          }
        case "id" =>
          _tid = Opt(jp.getText)
        case "text" =>
          _twitText = Opt(jp.getText)
        case "created_at" =>
          _twitCreatedAt = Opt(jp.getText)
        case "in_reply_to_status_id" =>
          _replyTwit = Opt(jp.getText)
          // NB returns "null", 4 characters, for null in JSON!
          // can we enforce actual null, perhaps without getText?
          // println("reply twit:"+_replyTwit.get+" is null:"+(_replyTwit.get == null)+"length:"+_replyTwit.get.length)
        case "in_reply_to_user_id" =>
          _replyUser = Opt(jp.getText)
        case _ => ()
      }
    }

    val uid: Int = getInt(_uid, "user id")
    val screenName: String = getString(_screenName, "user screen name")
    val statusesCount: Int = getInt(_statusesCount, "statuses count")
    val friendsCount: Int = getInt(_friendsCount, "friends count")
    val userTime: DateTime = getDateTime(_userCreatedAt, "user time")
    val utcOffsetInt = getIntOpt(_utcOffset,"utc offset")
    val utcOffset: Option[UTCOffset] = utcOffsetInt match {
      case Some(x) => Some((x / 3600).toByte)
      case _ => None
    }

    val tid: Long = getLong(_tid,"twit id")
    val twitText: String = unescapeHtml(getString(_twitText,"twit text"))
    val twitTime: DateTime = getDateTime(_twitCreatedAt,"twit time")
    val replyTwit: Option[TwitID] = getLongOpt(_replyTwit,"reply twit id")
    val replyUser: Option[UserID] = getIntOpt(_replyUser,"reply user id")

    // println(name+" "+twitTime+" ["+twitCreatedAt+"] "+"tid="+tid+", uid="+uid+
    //   showOption(", reply_uid=",replyUser)+showOption(", reply_tid=",replyTwit))

    val uRes = User(uid, name, screenName, statusesCount, friendsCount, userTime, location, utcOffset)
    val replyTwitOpt: Option[ReplyTwit] =
        replyUser match {
          case Some(ruid) => Some(ReplyTwit(tid,replyTwit,ruid))
          case _ =>
            if (!replyTwit.isEmpty)  // have nonEmpty in 2.8
              throw BadStatus("replyTwit nonempty while replyUser empty")
            else None
        }
    val tRes = Twit(tid, uid, twitTime, twitText, replyTwitOpt)
    UserTwit(uRes,tRes)
  }
  catch {
    case e: JsonParseException => err.println("*** Jackson JSON Error:"+e)
      throw BadStatus("*** Jackson JSON Error:"+e)
  } // end parse

  def apply(s:String) = parse(s)

  def getString(s:Option[String], name: String) = {
    s getOrElse { throw BadStatus("empty "+name) }
  }

  def getInt(s:Option[String], name: String) = {
    val meat = s getOrElse { throw BadStatus("empty "+name) }
    try {
      meat.toInt
    } catch {
      case _:NumberFormatException => throw BadStatus("bad Int in "+name)
    }
  }
  def getLong(s:Option[String], name: String) = {
    val meat = s getOrElse { throw  BadStatus("empty "+name) }
    try {
      meat.toLong
    } catch {
      case _:NumberFormatException => throw BadStatus("bad Long in "+name)
    }
  }

  def getDateTime(s:Option[String], name: String) = {
    val meat = s getOrElse { throw BadStatus("empty "+name) }
    try {
      dateTimeFmt.parseDateTime(meat)
    } catch {
      case _:IllegalArgumentException => throw BadStatus("bad DateTime in "+name)
    }
  }

  def getIntOpt(s:Option[String], name: String) =
    s match {
      case Some(meat) =>
        try {
          Some(meat.toInt)
        } catch {
          case _:NumberFormatException => throw BadStatus("bad Int Opt in "+name+"["+meat+"]")
        }
      case _ => None
    }
  def getLongOpt(s:Option[String], name: String) =
    s match {
      case Some(meat) =>
        try {
          Some(meat.toLong)
        } catch {
          case _:NumberFormatException => throw BadStatus("bad Long Opt in "+name+"["+meat+"]")
        }
      case _ => None
    }

  def Opt(s: String): Option[String] =
    s match {
      case null | "" | "null" => None
      case x => Some(x)
    }
}