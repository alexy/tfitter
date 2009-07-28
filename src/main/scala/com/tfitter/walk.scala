package com.tfitter

import db.types._
import db.{Twit}
import db.{TwitterBDB,BdbArgs,BdbFlags}
import scala.List
import System.err
import scala.collection.mutable.{Map=>UMap}
import java.io.{ObjectOutputStream,FileOutputStream}
import java.io.{File,ObjectInputStream,FileInputStream}

// case class DialogCount1(u1: UserID, u2: UserID, n12: TwitCount) //, n21: TwitCount

@serializable
@SerialVersionUID(1L)
class Repliers {
  type RepCount = UMap[UserID,(TwitCount,TwitCount)]
  var reps: UMap[UserID, RepCount] = UMap.empty

  // TODO here goes our favorite non-autovivification,
  // a question is, whether this can be made into auto-
  def addTwit(twit: Twit): Unit = {
    twit.reply match {
      case None =>
      case Some(reply) =>
        val uid = twit.uid
        val ruid = reply.replyUser
        val tinc = if (reply.replyTwit.isEmpty) 0 else 1
        val u = reps.get(uid)
           u match {
             case Some(repcount) => repcount.get(ruid) match {
               case Some((numUser,numTwit)) => {
                 val x = reps(uid)(ruid)
                 reps(uid)(ruid) = (x._1 + 1, x._2 + tinc)
               }
               case _ => reps(uid)(ruid) = (1, tinc)
             }
             case _ => reps(uid) = UMap(ruid->(1, tinc))
           }
    }
  }

  def toPairs1 = {
    val ll = reps.toList.map {
      case (uid,repcount) => repcount.toList map {
            case (ruid,(n12,t12)) => (uid,ruid,n12,t12) }
      }
    List.flatten(ll)
  }

  def toPairs2 = toPairs1 map { case (u1,u2,n12,t12) =>
    val (n21,t21) = reps get u2 match {
      case Some(repmap) => repmap get u1 getOrElse (0,0)
      case _ => (0,0)
    }
    val score = (n12-t12 + n21-t21) * 10 + (n12 min n21) * 100 + n12 + n21 + t12 + t21
    (u1,u2,n12,n21,t12,t21,score)
  }

  def topPairs = toPairs2 sort (_._7 > _._7)

  def showTopPairs(n: Int) = (topPairs take n) foreach { e =>
    println("%d %d %d %d %d %d %d" format (e._1, e._2, e._3, e._4, e._5, e._6, e._7))
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
      false,  // deferred write
      false   // noSync
    )

    val bdbEnvPath = if (args.length > 0) args(0) else Config.bdbEnvPath
    val bdbCacheSize = Some(256*1024*1024L) // None
    
    val bdbArgs = {
      import Config.{bdbStoreName}
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
        if (i % 100000 == 0) err.print('.')
      }

      if (reps.reps.size <= 100) {
        println(reps)
        println(reps.toPairs1)
        println(reps.toPairs2)
      }
      reps.showTopPairs(100)
      // TODO dump the full list into a file or BDB
      // TODO gather top 50000 different from top pairers
      val serName = "repliers.ser"
      err.print("writing pairs into "+serName+"... ")
      val oser = new ObjectOutputStream(new FileOutputStream(serName));
      oser.writeObject(reps)
      oser.close
      err.println("done")

      // to read back:
      // val file = new File("um.ser")
      // val ins = new ObjectInputStream(new FileInputStream(file))
      // Deserialize the object
      // val um1: Um = ins.readObject.asInstanceOf[Um]
      // ins.close

    }
    finally {
      tdb.close
    }
  }
}


object Dessert {
  def main(args: Array[String]) {
     // to read back:
     val file = new File("repliers.ser")
     val ins = new ObjectInputStream(new FileInputStream(file))
     val reps: Repliers = ins.readObject.asInstanceOf[Repliers]
     ins.close
     err.println("deserialized repliers:")
     // print all line by line
     // for ((u1,u2s) <- reps.reps; (u2,(t,u)) <- u2s) {
     //   println("%d->%d: %d %d" format (u1,u2,t,u))
     // }
     reps.showTopPairs(100)
   }
}
