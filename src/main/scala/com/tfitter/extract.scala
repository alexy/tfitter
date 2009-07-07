package com.tfitter

import System.err
import java.io.PrintStream
import org.codehaus.jackson._
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
    val factory: JsonFactory = new JsonFactory

    
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

              // do we need throttling here?
              // err.println("parser "+id+" ["+self+"] sends its inserter "+inserter.id+" ["+inserter+"] twit "+tRes.tid)
              react {
                case a: Inserter =>
                  inserter ! UserTwit(uRes,tRes)
              }
            }
            catch {
              case BadStatus(reason) => err.println("*** BAD STATUS:"+reason+" \nline:"+s)
              case e: JsonParseException => err.println("*** Jackson JSON Error:"+e)
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
      err.println("Inserter "+id+" started, object "+self+",\n"+
              "  talking to parser "+extractor.id + " ["+extractor+"]")
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