package com.tfitter

import org.joda.time.DateTime
import org.scala_tools.time.Imports._
import com.tfitter.db.{Twit, TwitterDB, TwitterBDB, TwIterator}
// import com.tfitter.db.types._
import org.suffix.util.bdb.{BdbFlags, BdbArgs}
import System.err

object WalkTwits extends optional.Application {
  def main(
    envName: Option[String],
    storeName: Option[String],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],
    maxTwits: Option[Long],
    twitsProgress: Option[Long],
    args: Array[String]) = {

    val bdbEnvPath   = envName getOrElse   "bdb"
    val bdbStoreName = storeName getOrElse "twitter"
    err.println("twitsProgress => "+twitsProgress)
    err.println("maxTwits => "+maxTwits)
    
    System.exit(1)
    
    val bdbCacheSize = cacheSize match {
      case Some(x) => Some((x*1024*1024*1024).toLong)
      case _ => None // Config.bdbCacheSize
    }
    val bdbFlags = BdbFlags(
      allowCreate   getOrElse false,
      readOnly      getOrElse true,
      transactional getOrElse false,
      deferredWrite getOrElse false,
      noSync        getOrElse false
    )
    val bdbArgs = BdbArgs(bdbEnvPath,bdbStoreName,bdbFlags,bdbCacheSize)

    val tdb: TwitterDB = new TwitterBDB(bdbArgs)
    
    // the day before any twits
    var lastTwitTime = new DateTime("2008-01-01")
    
    var twitCount = 0
    for (t <- tdb.allTwits /*.take(atMost.toInt)*/) {
      twitCount += 1
      if (!twitsProgress.isEmpty && twitCount % twitsProgress.get == 0) err.println(lastTwitTime)
      if (lastTwitTime < t.time) lastTwitTime = t.time
    }
    println(lastTwitTime)
  }
}
