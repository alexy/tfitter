package com.tfitter

import System.err

import org.suffix.util.bdb.{BdbArgs,BdbFlags}
import org.suffix.util.input.GlueSources

import com.tfitter.db.{UserTwit,DBError,TwitterBDB} // for Berkeley DB backend
import com.tfitter.json.TwitExtract

import scala.actors.Actor
import scala.actors.Actor._

case class BadStatus(reason: String) extends Exception(reason)

object Status {     
  sealed abstract class ParserMessage()
  case class Parse(s: String) extends ParserMessage
  case object EndOfInput extends ParserMessage // caused class not found exception!

  // this should go into org.suffix.util        
  def showOption[T](prefix: String, x: Option[T]): String = x match {
    case Some(s) => prefix+s.toString
    case _ => ""
  }    
  
  class ReadLines(files: Array[String], numCallers: Int, progress: Boolean) extends Actor { 
    
       
    def act = {
      err.println("ReadLines started, object "+self)
      val lines = GlueSources.glueFilesLineNums(files,progress)
      
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
                  // TODO lines.close?
                  err.println("ReadLines exiting, having "+mailboxSize+" messages left.")
                  // Runtime.getRuntime.addShutdownHook(exit)
                  exit
                }
              } else {
                try {
                  var break = false
                  do {
                    lines.next match {
                      case (line,lineNumber) if !line.trim.isEmpty =>
                        a ! Parse(line)
                        break = true
                        if (progress && lineNumber % 10000 == 0) err.print('.')
                      case _ =>
                    }
                  } while (!break)
                } catch {
                  case e => err.println("*** CANNOT GET LINE:"+e)
                  a ! EndOfInput
                }
              }
            }
          case msg => err.println("ReadLines unhandled message:"+msg)
        }
      }
     /* if (progress) */ err.println; err.println("ReadLines ended.")
    }
  }  
  
  class JSONExtractor(val id: Int, val readLines: ReadLines, val inserter: Inserter) extends Actor {
    
    def act = {
      err.println("Parser "+id+" started, object "+self+",\n"+
      "  talking to Inserter "+inserter.id+" ["+inserter+"],\n"+
      "  and readLines "+readLines)
      readLines ! self
      loop {
        react {
          case Parse(s) => try {
              val ut = TwitExtract(s)
              react {
                case a: Inserter =>
                  inserter ! ut
              }
            }
            catch {
              // TODO Now, for delete statuses, we must return None from JSON
              case BadStatus(reason) => err.println("*** BAD STATUS CAUGHT:"+reason+" \nline:"+s)
            }
            finally {
              readLines ! self
            }
          case EndOfInput => { 
            inserter ! EndOfInput
            err.println("Parser "+id+" exiting, having "+mailboxSize+" messages left.")
            exit
          }
          // inserter's message expected nested above
          // may be in queue already, not handled at this top level,
          // so no "unhandled" catch-all anymore:
          // case msg => err.println("Parser "+id+" unhandled message:"+msg)
          }
        }
        err.println("Parser "+id+" ended.")
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
            err.println("Inserter "+id+" exiting, having "+mailboxSize+" messages left.")
            exit
          }
          case msg => err.println("Inserter "+id+" unhandled message:"+msg)
        }
      }
      err.println("Inserter "+id+" ended.")
    }
  }


  class BdbInserter(override val id: Int, bdbArgs: BdbArgs) extends Inserter(id) {
    // apparently there's no difference whether this is inside or oustide act?
    val tdb = new TwitterBDB(bdbArgs)
    def act() = {
      err.println("Inserter "+id+" started, object "+self+",\n"+
              "  talking to parser "+extractor.id + " ["+extractor+"]")
      extractor ! self
      loop {
        react {
          case ut @ UserTwit(_,_) => {
            try {
              tdb.insertUserTwit(ut)
            } catch {
              case e => err.println("ERROR: "+e)
            }

            extractor ! self
          }
          case EndOfInput => {
            err.println("Inserter "+id+" flushing to disk...")
            tdb.close
            err.println("Inserter "+id+" exiting, having "+mailboxSize+" messages left.")
            exit
          }
          case msg => err.println("Inserter "+id+" unhandled message:"+msg)
        }
      }
    }
  }
}

// val line = scala.io.Source.fromFile("/s/data/twitter/samples/2174151307.json").getLines.next