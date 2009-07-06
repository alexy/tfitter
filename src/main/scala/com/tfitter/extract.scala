package com.tfitter

import System.err
import java.io.PrintStream
import org.apache.commons.lang.StringEscapeUtils.unescapeHtml

import com.tfitter.db.types._
import com.tfitter.db.{User,Twit,ReplyTwit,UserTwit,JdbcArgs,TwitterPG,DBError}

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

    // NB asInstanceOf[T] gets erased and needs to be rewritten
    def extractInt(m: Map[String,Any], field: String, whose: String): Int = 
      m.get(field) match {
        case Some(x:Int) => x
        case _ => throw BadStatus(whose+" has no "+field)
      }
      
    def extractLong(m: Map[String,Any], field: String, whose: String): Long = 
      m.get(field) match {
        case Some(x:Int)  => x.toLong
        case Some(x:Long) => x
        case _ => throw BadStatus(whose+" has no "+field)
      }

    def extractString(m: Map[String,Any], field: String, whose: String): String =
      m.get(field) match {
        case Some(x:String) => x
        case _ => throw BadStatus(whose+" has no "+field)
      }

    def extractNullableInt(m: Map[String,Any], field: String, whose: String)
      : Option[Int] =
      m.get(field) match {
        case Some(null) => None
        case Some(x:Int) => Some(x)
        case _ => throw BadStatus(whose+" has no nullable Int "+field)
      }

    def extractNullableLong(m: Map[String,Any], field: String, whose: String)
      : Option[Long] =
      m.get(field) match {
        case Some(null) => None
        case Some(x:Int) => Some(x.toLong)
        case Some(x:Long) => Some(x)
        case _ => throw BadStatus(whose+" has no nullable Long "+field)
      }

    def extractNullableString(m: Map[String,Any], field: String, whose: String)
      : Option[String] =
      m.get(field) match {
        case Some(null) => None
        case Some(x:String) => Some(x)
        case _ => throw BadStatus(whose+" has no nullable String "+field)
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
              val uid: Int = extractInt(user,"id","user")        
              val name: String = extractString(user,"name","user")
              val screenName: String = extractString(user,"screen_name","user")
              val statusesCount: Int = extractInt(user,"statuses_count","user")
              val friendsCount: Int = extractInt(user,"friends_count","user")
              val userCreatedAt: String = extractString(user,"created_at","user")
              val userTime: DateTime = try { dateTimeFmt.parseDateTime(userCreatedAt) }
                catch { case _: IllegalArgumentException => throw BadStatus("cannot parse user time") }
              val location: Option[String] = extractNullableString(user,"location","user")
              val utcOffsetInt: Option[Int] = extractNullableInt(user,"utc_offset","user")
              val utcOffset: Option[UTCOffset] = utcOffsetInt match {
                case Some(x) => Some((x / 3600).toByte)
                case _ => None
              }
              // twit
              val tid: Long = extractLong(twit,"id","twit")
              val twitCreatedAt: String = extractString(twit,"created_at","twit")
              val twitTime: DateTime = try { dateTimeFmt.parseDateTime(twitCreatedAt) }
              catch { case _: IllegalArgumentException => throw BadStatus("cannot parse twit time") }
              val twitText: String = unescapeHtml(extractString(twit, "text", "twit"))
              val replyTwit: Option[TwitID] = extractNullableLong(twit,"in_reply_to_status_id","twit")
              val replyUser: Option[UserID] = extractNullableInt(twit,"in_reply_to_user_id","twit")
      
        
              // println(name+" "+twitTime+" ["+twitCreatedAt+"] "+"tid="+tid+", uid="+uid+
              //   showOption(", reply_uid=",replyUser)+showOption(", reply_tid=",replyTwit))
        
              val uRes = User(uid, name, screenName, statusesCount, friendsCount, userTime, location, utcOffset)
              val replyTwitOpt: Option[ReplyTwit] =
                try {
                  replyUser match {
                    case Some(ruid) => Some(ReplyTwit(tid,replyTwit,ruid))
                    case _ =>
                      if (!replyTwit.isEmpty)  // nonEmpty is in 2.8
                        throw BadStatus("replyTwit nonempty while replyUser empty")
                      else None
                  }
                } catch { case _: ClassCastException =>
                  // throw BadStatus
                  error(":***CAST ERROR***: tid="+tid+", uid="+uid+
                          ", replyTwit="+replyTwit+", replyUser="+replyUser+" => "+
                    twitText)
                }
              val tRes = Twit(tid, uid, twitTime, twitText, replyTwitOpt)

              // do we need throttling here?
              // err.println("parser "+id+" ["+self+"] sends its inserter "+inserter.id+" ["+inserter+"] twit "+tRes.tid)
              react {
                case a: Inserter =>
                  inserter ! UserTwit(uRes,tRes)
              }
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
          // inserter's message may be in queue already, not handled here,
          // so no "unhandled" catch-all anymore:
          // case msg => err.println("Parser "+id+" unhandled message:"+msg)
          }
        }
      }
    }

  abstract class Inserter(val id: Int) extends Actor {
    var extractor: JSONExtractor = null
    def setExtractor(e: JSONExtractor): Unit = extractor = e
  }
  
  class Printer(override val id: Int) extends Inserter(id) {
    def act() = {
      err.println("Inserter "+id+" started, object "+self)
      extractor ! self      
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
            extractor ! self
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

  class PGInserter(override val id: Int, jdbcArgs: JdbcArgs) extends Inserter(id) {
    val tdb = new TwitterPG(jdbcArgs)
    def act() = {
      err.println("Inserter "+id+" started, object "+self)
      extractor ! self
      loop {
        react {
          case ut @ UserTwit(_,_) => { /* ut @ UserTwit(user,twit)
             print(user.name+" "+twit.time+": "+twit.text+
              ", tid="+twit.tid+", uid="+user.uid)
             twit.reply match {
               case Some(r) => println(", reply_twit="+r.replyTwit +
               ", reply_user="+r.replyUser)
               case _ => ()
             }
            println
            */
            try {
              // val t = tdb.TwitPG(twit.tid)
              // t put twit
              tdb.insertUserTwit(ut)
            } catch {
              case DBError(msg) => err.println("DB ERROR: "+msg)
            }

            extractor ! self
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

  // simple testing version where inserters just print their inputs
  def printer: Array[String] => Unit = { args => 

    val numThreads = Config.numCores

    // make this a parameter:
    val showingProgress = true

    Console.setOut(new PrintStream(Console.out, true, "UTF8"))
    err.println("[this is stderr] Welcome to Twitter Gardenhose JSON Extractor in IDEA")
    // Console.println("this is console")

    val readLines = new ReadLines(args(0),numThreads,showingProgress)

    // before I added type annotation List[Inserter] below,
    // I didn't realize I'm not using a List but get a Range...  added .toList below
    val inserters: List[Printer] = (0 until numThreads).toList map (new Printer(_))

    val parsers: List[JSONExtractor] = inserters map (ins => new JSONExtractor(ins.id,readLines,ins))

    (parsers zip inserters) foreach { case (parser, inserter) =>
      inserter.setExtractor(parser) }
    
    readLines.start
    inserters foreach (_.start)
    parsers foreach (_.start)
  }

  
  def inserter(args: Array[String]) {
    import la.scala.sql.rich.RichSQL._

    val numThreads = Config.numCores
    val jdbcArgs = {
      import Config.{jdbcUrl,jdbcUser,jdbcPwd,rangeTable,twitTable,replyTable}
      JdbcArgs(jdbcUrl,jdbcUser,jdbcPwd,rangeTable,twitTable,replyTable)
    }

    
    // make this a parameter:
    val showingProgress = true
    
    Console.setOut(new PrintStream(Console.out, true, "UTF8"))
    err.println("[this is stderr] Welcome to Twitter Gardenhose JSON Extractor in IDEA")
    // Console.println("this is console")

    // don't even need ti import java.sql.DriverManager for this,
    // magic -- NB see excatly what it does:
    val dbDriver = Class.forName("org.postgresql.Driver")

    val readLines = new ReadLines(args(0),numThreads,showingProgress)
    
    // before I added type annotation List[Inserter] below, 
    // I didn't realize I'm not using a List but get a Range...  added .toList below
    val inserters: List[PGInserter] = (0 until numThreads).toList map (new PGInserter(_,jdbcArgs))

    val parsers: List[JSONExtractor] = inserters map (ins => new JSONExtractor(ins.id,readLines,ins))

    (parsers zip inserters) foreach { case (parser, inserter) =>
      inserter.setExtractor(parser) }

    readLines.start
    inserters foreach (_.start)
    parsers foreach (_.start)
  }

  def main(args: Array[String]) =
    inserter(args)
    // printer(args)
}

// val line = scala.io.Source.fromFile("/s/data/twitter/samples/2174151307.json").getLines.next