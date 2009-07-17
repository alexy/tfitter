package com.tfitter

import db.types._
import db.{Twit}
import db.{TwitterBDB,BdbArgs,BdbFlags}
import System.err
import scala.collection.mutable.{Map=>UMap}

// case class DialogCount1(u1: UserID, u2: UserID, n12: TwitCount) //, n21: TwitCount

class Repliers {
  type RepCount = UMap[UserID,TwitCount]
  var reps: UMap[UserID, RepCount] = UMap.empty

  // TODO here goes our favorite non-autovivification,
  // a question is, whether this can be made into auto-
  def addTwit(twit: Twit): Unit = {
    twit.reply match {
      case None =>
      case Some(reply) =>
        val uid = twit.uid
        val ruid = reply.replyUser
        val u = reps.get(uid)
           u match {
             case Some(repcount) => repcount.get(ruid) match {
               case Some(count) => reps(uid)(ruid) += 1
               case _ => reps(uid)(ruid) = 1
             }
             case _ => reps(uid) = UMap(ruid->1)
           }
    }
  }

  def toPairs1 = {
    val ll = reps.toList.map {
      case (uid,repcount) => repcount.toList map {
            case (ruid,n12) => (uid,ruid,n12) }
      }
    List.flatten(ll)
  }

  def toPairs2 = toPairs1 map { case (u1,u2,n12) =>
    val n21 = reps get u2 match {
      case Some(repmap) => repmap get u1 getOrElse 0
      case _ => 0
    }
    val score = (n12 min n21) * 100 + n12 + n21
    (u1,u2,n12,n21,score)
  }

  def topPairs = toPairs2 sort (_._5 > _._5)

  def showTopPairs(n: Int) = (topPairs take n) foreach { e =>
    println("%d %d %d %d %d" format (e._1, e._2, e._3, e._4, e._5))
  }
  override def toString = reps.toString
}

object Walk {
  def main(args: Array[String]) {
    // non-transactional read-only
    val bdbFlags = BdbFlags(
      false,  // allowCreate
      true,   // readOnly
      false,  // transactional
      false   // deferred write
    )

    val bdbCacheSize = None
    
    val bdbArgs = {
      import Config.{bdbEnvPath,bdbStoreName,bdbCacheSize}
      BdbArgs(bdbEnvPath,bdbStoreName,bdbFlags,bdbCacheSize)
    }

    err.println("Twitter BDB contains the following users:")
    val tdb = new TwitterBDB(bdbArgs)
    try {
      // for (x <- tdb.allUserStats) println(x)
      // val mu: List[UserStats] = tdb.allUserStatsList
      //   println(mu mkString "\n")

      var reps = new Repliers
      for ((t,i) <- tdb.allTwits.zipWithIndex) {
        reps.addTwit(t)
        if (i % 10000 == 0) err.print('.')
      }

      println(reps)
      println(reps.toPairs1)
      println(reps.toPairs2)
      reps.showTopPairs(100)
    }
    finally {
      tdb.close
    }
  }
}