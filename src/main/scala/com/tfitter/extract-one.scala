package com.tfitter

import System.err

import db.{UserTwit,DBError}
import org.suffix.util.bdb.{BdbArgs,BdbFlags}
import db.TwitterBDB // for Berkeley DB backend
import json.TwitExtract

import org.suffix.util.input.GlueSources

// actorless, single-core

abstract class LoadStatuses { 
  
  def doUserTwit(ut: UserTwit): Unit    
  
  def load(files: Array[String], progress: Option[Long]) = { 
  
    val lines = GlueSources.glueFilesLineNums(files)
    
    for (line_num <- lines) {
      line_num match {
        case (line,lineNumber) if !line.trim.isEmpty =>
          try { 
              val ut = TwitExtract(line)

              doUserTwit(ut)

              progress match {
                case Some(n) if lineNumber % n == 0 => err.print('.')
                case _ =>
              }
          } catch {
            // TODO Now, for delete statuses, we must return None from JSON
            case BadStatus(reason) => // err.println("*** BAD STATUS CAUGHT:"+reason+" \nline:"+line)
          }
        case _ => // next
      }
    }
  }
  
  def finish: Unit = {} 
}

class BdbInsertStatuses(bdbArgs: BdbArgs) extends LoadStatuses {
      
  val tdb = new TwitterBDB(bdbArgs)

  override def doUserTwit(ut: UserTwit): Unit = {
      try {
        tdb.insertUserTwit(ut)
      } catch {
        case e => err.println("ERROR: "+e)
      }
  }

  override def finish: Unit = {
    tdb.close
  }
}


class PrintStatuses extends LoadStatuses {
  override def doUserTwit(ut: UserTwit): Unit = {
     val UserTwit(user, twit) = ut
     print(user.name+" "+twit.time+": "+twit.text+
      ", tid="+twit.tid+", uid="+user.uid)
     twit.reply match {
       case Some(r) => println(", reply_twit="+r.replyTwit +
       ", reply_user="+r.replyUser)
       case _ =>
     }    
  }
}