package com.tfitter

import java.io.PrintStream

import com.twitter.commons.Json

object Status {
  case class BadStatus(reason: String) extends Exception(reason)
  
  def main(args: Array[String]){
    Console.setOut(new PrintStream(Console.out, true, "UTF8"))
    
    // for extracting the time
    val dateTimeFmt = org.joda.time.format.DateTimeFormat.forPattern("EEE MMM dd kk:mm:ss Z YYYY")
    
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
          
    def showOption[T](x: Option[T]): String = x match {
      case Some(s) => s.toString
      case _ => ""
    }
    
    for (line <- scala.io.Source.fromFile(args(0),"UTF-8").getLines) {
      try {
        val tweet = Json.parse(line) match {
          case m: Map[String,Any] => m
          // we need continue here and below, not bail on error
          case _ => throw BadStatus("Not a Twitter status in JSON format:"+line)
        }
        
        // user 
        val user = {tweet.get("user") match {
          case Some(x) => x
          case _ => error("no user data inside tweet")
        }} match {
          case m: Map[String,Any] => m
          case _ => throw BadStatus("user data is not a map")
        }
        
        // user name
        val name: String = extractField(user,"name","user")
        
        // user screen_name
        val screen_name: String = extractField(user,"screen_name","user")
        
        // tweet time
        val created_at: String = extractField(tweet,"created_at","tweet")
        val dt = dateTimeFmt.parseDateTime(created_at)
        
        val uid: Int = extractField(user,"id","user")
      
        // tid
        val tid: Long = extractField(tweet,"id","tweet")
      
        // reply_tid
        val reply_tid: Option[Long] = extractNullableField(tweet,"in_reply_to_status_id","tweet")
        
        // reply_uid
        val reply_uid: Option[Int] = extractNullableField(tweet,"in_reply_to_user_id","tweet")

        // fails on empty:
        // bad status:tweet in_reply_to_status_id is not a java.lang.String
        // val reply_uid: Option[Int] =
        //    extractField[String](tweet,"in_reply_to_user_id","tweet") match {
        //    case "" => None
        //    case s => try { Some(s.toInt) } catch {
        //      case _: NumberFormatException => throw BadStatus(
        //        "in_reply_to_user_id is a non-empty non-number "+s)
        //    }
        // }

        // this works for non-empty string, but raises error during runtime for empty one:
        // bad status:tweet in_reply_to_status_id is not a scala.runtime.Nothing$
        // val reply_uid: Option[Int] = (extractField(tweet,"in_reply_to_user_id","tweet"): Any) match {
        //   case i: Int => Some(i)
        //   case _ => None
        // }

        // this works with out own BadStatus exception overhead on empty
        // val reply_uid: Option[Int] = try { 
        //   val r: Int = extractField(tweet,"in_reply_to_user_id","tweet")
        //   Some(r) }
        //   catch {
        //    // com.tfitter.Status$BadStatus: tweet in_reply_to_user_id is not a scala.runtime.Nothing$
        //    case e => /*println(e);*/ None
        //   }
        
        println(name+" "+dt+" ["+created_at+"] "+"tid="+tid+", uid="+uid+
          ", reply_uid="+showOption(reply_uid)+", reply_tid="+showOption(reply_tid))
      }
      catch {
        case BadStatus(reason) => println("bad status:"+reason+" "+line)
      }
    }
  }
}