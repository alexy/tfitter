package com.tfitter

import System.err
import java.io.PrintStream

import com.tfitter.db.types._
import com.tfitter.db.{User,Twit}

import scala.io.Source
import scala.actors.Actor
import scala.actors.Actor._

object Status {
  
  case class BadStatus(reason: String) extends Exception(reason)
   
  abstract case class ParserMessage()
  case class Parse(s: String) extends ParserMessage
  
  
  class ReadLines(source: String, progress: Boolean) extends Actor {    
    def act = {
      err.println("ReadLines started")
      val lines = Source.fromFile(source,"UTF-8").getLines.zipWithIndex
      
      loop {
        react {
          case a: Actor =>
            { 
              // err.println("got request from actor "+a)
              // if (!lines.hasNext) // what?
              try {
                val (line,lineNumber) = lines.next
                a ! Parse(line)        
                if (progress && lineNumber % 10000 == 0) err.print('.')
              } catch {
                case _ => err.println("cannot get line")
              }
            }
          case msg => err.println("ReadLines unhandled message:"+msg)
        }
      }
     if (progress) err.println; err.println("ReadLines ended.")
    }
  }  
  
  class JSONExtractor(id: Int, readLines: ReadLines) extends Actor {
    
    import com.twitter.commons.{Json,JsonException}
    import org.joda.time.DateTime
    import org.joda.time.format.DateTimeFormat
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
          
    def showOption[T](prefix: String, x: Option[T]): String = x match {
      case Some(s) => prefix+s.toString
      case _ => ""
    }    
    
    // // anyToMap without cast, but with erasure warning
    // def anyToMap: Any => Map[String,Any] = {
    //   case m: Map[String,Any] => m
    //   case _ => throw BadStatus("anyToMap: not a Map[String,Any]")
    // }

    // anyToMap with the cast, but without the erasure warning
    def anyToMap: Any => Map[String,Any] = {
      case m: Map[_,_] => try { m.asInstanceOf[Map[String,Any]] }
        catch { case _: ClassCastException => throw BadStatus("data is not a Map[String,Any]") }
          // we need continue here and below, not bail on error
      case _ => throw BadStatus("Not a Twitter status as Map[_,_]")
    }
    
    def act = {
      err.println("Parser "+id+" started")
      readLines ! self
      loop {
        react {
          case Parse(s) => try {
              // Twitter forgot to scrub ^? in some user's location/description,
              // see status 2174151307
              // entering chars by code, equivalent methods:
              // "\u007f"
              // 127.toChar.toString
              val line = s.replaceAll("\u007f", "")
        
              val parsed = try { Json.parse(line) } catch {
                // case JsonException(reason) => throw BadStatus("Garbled JSON format: "+reason)
                case JsonException(reason) => throw BadStatus("Garbled JSON format: "+reason) 
              }
                
              // version with userAny, anyToMap
              val twit = anyToMap(parsed)
              val userAny = twit.get("user") getOrElse { throw BadStatus("no user data inside twit") }
              // ->: anyToMap
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
        
              // val uRes = User(uid, name, screenName, statusesCount, userTime, location, utcOffset)
              // val tRes = Twit(tid, twitTime, replyTwit, replyUser)
              // (uRes,tRes)
              readLines ! self
            }
            catch {
              case BadStatus(reason) => err.println("*** BAD STATUS:"+reason+" \nline:"+s)
            }
          case msg => err.println("JSONExtractor "+id+" unhandled message:"+msg)
          }
        }
      }
    }

  
  def main(args: Array[String]){
        
    val numParsers = 2
    
    // make this a parameter:
    val showingProgress = true
    
    Console.setOut(new PrintStream(Console.out, true, "UTF8"))
    // what do we do for stderr?
    // Console.setErr(new PrintStream(Console.err, true, "UTF8"))

    err.println("[this is stderr] Welcome to Twitter Gardenhose JSON Extractor")
    // Console.println("this is console")

    val readLines = new ReadLines(args(0),showingProgress)
    val parsers = (0 until numParsers) map (i => new JSONExtractor(i,readLines))
    
    readLines.start
    for (i <- 0 until numParsers) parsers(i).start
  }
}

// val line = scala.io.Source.fromFile("/s/data/twitter/samples/2174151307.json").getLines.next