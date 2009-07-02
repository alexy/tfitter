package com.tfitter

import actors.Actor
import io.Source
import System.err
import java.io.PrintStream

import com.tfitter.db.types._
import com.tfitter.db.{User,Twit,ReplyTwit,UserTwit}

import scala.io.Source
import scala.actors.Actor
import scala.actors.Actor._

object Status {
  
  case class BadStatus(reason: String) extends Exception(reason)
   
  sealed abstract class ParserMessage()
  case class Parse(s: String) extends ParserMessage
  case object EndOfInput extends ParserMessage // caused class not found exception!

  // this should go into la.scala.util        
  def showOption[T](prefix: String, x: Option[T]): String = x match {
    case Some(s) => prefix+s.toString
    case _ => ""
  }    
  
  class ReadLines(source: String, numCallers: Int, progress: Boolean) extends Actor {    
    def act = {
      err.println("ReadLines started, object "+self)
      val lines = Source.fromFile(source,"UTF-8").getLines.zipWithIndex
      
      var countDown = numCallers
      
      loop {
        react {
          case a: JSONExtractor =>
            { 
              // err.println("got request from actor "+a)
              if (!lines.hasNext) {
                err.println("end of input, informing parser "+a.id)
                a ! EndOfInput
                countDown -= 1
                if (countDown == 0) {
                  err.println("ReadLines exiting")
                  exit()
                }
              } else {
                try {
                  val (line,lineNumber) = lines.next
                  a ! Parse(line)        
                  if (progress && lineNumber % 10000 == 0) err.print('.')
                } catch {
                  case _ => err.println("cannot get line")
                  a ! EndOfInput
                }
              }
            }
          case msg => err.println("ReadLines unhandled message:"+msg)
        }
      }
     if (progress) err.println; err.println("ReadLines ended.")
    }
  }  
  
  class JSONExtractor(val id: Int, val readLines: ReadLines, val inserter: Inserter) extends Actor {
    
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
      err.println("Parser "+id+" started, object "+self+",\n"+
      "  talking to Inserter "+inserter.id+" ["+inserter+"],\n"+
      "  and readLines "+readLines)
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
              val twitText: String = extractField(twit, "text", "twit")
              val replyTwit: Option[TwitID] = extractNullableField(twit,"in_reply_to_status_id","twit")
              val replyUser: Option[UserID] = extractNullableField(twit,"in_reply_to_user_id","twit")
      
        
              // println(name+" "+twitTime+" ["+twitCreatedAt+"] "+"tid="+tid+", uid="+uid+
              //   showOption(", reply_uid=",replyUser)+showOption(", reply_tid=",replyTwit))
        
              val uRes = User(uid, name, screenName, statusesCount, userTime, location, utcOffset)
              val replyTwitOpt: Option[ReplyTwit] = (replyTwit,replyUser) match {
                case (Some(rtid),Some(ruid)) => Some(ReplyTwit(tid,rtid,ruid))
                case _ =>
                  if (replyTwit.isEmpty != replyUser.isEmpty)
                    throw BadStatus("reply to twit id and user id nullity doesn't match")
                  else None
              }
              val tRes = Twit(tid, uid, twitTime, twitText, replyTwitOpt)

              // do we need throttling here?
              // err.println("parser "+id+" ["+self+"] sends its inserter "+inserter.id+" ["+inserter+"] twit "+tRes.tid)
              inserter ! UserTwit(uRes,tRes)              
            }
            catch {
              case BadStatus(reason) => err.println("*** BAD STATUS:"+reason+" \nline:"+s)
            }
            finally {
              readLines ! self
            }
          case EndOfInput => { 
            inserter ! EndOfInput
            err.println("Parser "+id+" exiting.")
            exit() 
          }
          case msg => err.println("Parser "+id+" unhandled message:"+msg)
          }
        }
      }
    }

  class Inserter(val id: Int) extends Actor {
    def act() = {
      err.println("Inserter "+id+" started, object "+self)
      loop {
        react {
          case UserTwit(user, twit) => {
             print(user.name+" "+twit.time+": "+twit.text+
              ", tid="+twit.tid+", uid="+user.uid)
             twit.reply match {
               case Some(r) => println(", reply_twit="+r.replyTwit +
               ", reply_user="+r.replyUser)
               case _ => ()
             }
            println
          }
          case EndOfInput => {
            err.println("Inserter "+id+" exiting.")
            exit()
          }
          case msg => err.println("Inserter "+id+" unhandled message:"+msg)
        }
      }
    }
  }
  
  
  def main(args: Array[String]) {
    
    val numThreads = Config.numCores
    
    // make this a parameter:
    val showingProgress = true
    
    Console.setOut(new PrintStream(Console.out, true, "UTF8"))
    err.println("[this is stderr] Welcome to Twitter Gardenhose JSON Extractor in IDEA")
    // Console.println("this is console")

    val readLines = new ReadLines(args(0),numThreads,showingProgress)
    
    // before I added type annotation List[Inserter] below, 
    // I didn't realize I'm not using a List but get a Range...  added .toList below
    val inserters: List[Inserter] = (0 until numThreads).toList map (new Inserter(_))

    val parsers: List[JSONExtractor] = inserters map (ins => new JSONExtractor(ins.id,readLines,ins))

    readLines.start
    inserters foreach (_.start)
    parsers foreach (_.start)
  }
}

// val line = scala.io.Source.fromFile("/s/data/twitter/samples/2174151307.json").getLines.next