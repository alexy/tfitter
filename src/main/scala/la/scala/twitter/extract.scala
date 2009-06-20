package com.tfitter

import java.io.PrintStream
import com.twitter.commons.{Json,JsonException}

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.tfitter.db.types._
import com.tfitter.db.{User,Twit}

object Status {
  
  case class BadStatus(reason: String) extends Exception(reason)
  
  def main(args: Array[String]){
    Console.setOut(new PrintStream(Console.out, true, "UTF8"))
    // what do we do for stderr?
    // Console.setErr(new PrintStream(Console.err, true, "UTF8"))

    System.err.println("this is stderr")
    Console.println("this is console")
    
    // for extracting the time
    val dateTimeFmt = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z YYYY")
    
    def extractField[T](m: Map[String,Any], field: String, whose: String)
      (implicit manifest: scala.reflect.Manifest[T]): T = 
      m.get(field) match {
        case Some(x) => x.asInstanceOf[T]
        case _ => throw BadStatus(whose+" has no "+field)
      }
      
    def extractNullableField[T](m: Map[String,Any], field: String, whose: String)
      (implicit manifest: scala.reflect.Manifest[T]): Option[T] = 
      m.get(field) match {
        case Some(null) => None
        case Some(x) => Some(x.asInstanceOf[T])
        case _ => throw BadStatus(whose+" has no "+field)
      }
          
    def showOption[T](prefix:String = "", x: Option[T]): String = x match {
      case Some(s) => prefix+s.toString
      case _ => ""
    }
    
    def anyToMap: Any => Map[String,Any] = {
      case m: Map[_,_] => try { m.asInstanceOf[Map[String,Any]] }
        catch { case _: ClassCastException => throw BadStatus("data is not a Map[String,Any]") }
          // we need continue here and below, not bail on error
      case _ => throw BadStatus("Not a Twitter status as Map[_,_]")
    }
    
    for (line <- scala.io.Source.fromFile(args(0),"UTF-8").getLines) {
      try {
        val twitAny = try { Json.parse(line) } catch {
          case JsonException(reason) => throw BadStatus("Garbled JSON format: "+reason) }
        val twit = anyToMap(twitAny)
        
        val userAny = twit.get("user") match {
          case Some(x) => x
          case _ => error("no user data inside twit")
        } // ->: anyToMap
        val user = anyToMap(userAny)

        // user
        val uid: Int = extractField(user,"id","user")
        val name: String = extractField(user,"name","user")
        val screenName: String = extractField(user,"screen_name","user")
        val statusesCount: Int = extractField(user,"statuses_count","user")
        val userCreatedAt: String = extractField(user,"created_at","user")
        val userTime: DateTime = try { dateTimeFmt.parseDateTime(userCreatedAt) }
          catch { case _: IllegalArgumentException => throw BadStatus("cannot parse user time") }
        val location: String = extractField(user,"location","user")
        // can't seem to declare it :UTCOffset right away: / gives type error!
        // also have to give explicit type parameter, otherwise were getting,
        // error: value / is not a member of Nothing
        val utcOffsetInt: Int = extractField[Int](user,"utc_offset","user") / 3600
        val utcOffset: UTCOffset = utcOffsetInt.asInstanceOf[UTCOffset]
        
        // twit
        val tid: Long = extractField(twit,"id","twit")
        val twitCreatedAt: String = extractField(twit,"created_at","twit")
        val twitTime: DateTime = try { dateTimeFmt.parseDateTime(twitCreatedAt) }
        catch { case _: IllegalArgumentException => throw BadStatus("cannot parse twit time") }
        val replyTwit: Option[Long] = extractNullableField(twit,"in_reply_to_status_id","twit")
        val replyUser: Option[Int] = extractNullableField(twit,"in_reply_to_user_id","twit")
      
        
        println(name+" "+twitTime+" ["+twitCreatedAt+"] "+"tid="+tid+", uid="+uid+
          showOption(", reply_uid=",replyUser)+showOption(", reply_tid=",replyTwit))
          
        val uRes = User(uid, name, screenName, statusesCount, userTime, location, utcOffset)
        val tRes = Twit(tid, twitTime, replyTwit, replyUser)
        
        (uRes,tRes)
      }
      catch {
        case BadStatus(reason) => System.err.println("*** BAD STATUS:"+reason+" "+line)
      }
    }
  }
}