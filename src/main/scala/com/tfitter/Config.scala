package com.tfitter

import la.scala.util.Properties
import System.err

object Config {
  val numCores     = Properties.getInt("number.cores",1)
  val jdbcUrl      = Properties.getString("jdbc.url","jdbc:postgresql:twitter")
  val jdbcUser     = Properties.getString("jdbc.user","")
  val jdbcPwd      = Properties.getString("jdbc.pwd","")
  val rangeTable   = Properties.getString("range.table","range")
  val twitTable    = Properties.getString("twit.table","twit")
  val replyTable   = Properties.getString("reply.table","reply")
  val bdbEnvPath   = Properties.getString("bdb.env.path","./tmp")
  val bdbStoreName = Properties.getString("bdb.store.name","twitter")
  val bdbCacheSize = Some(Properties.getLong("bdb.cache.size",2147483647))
}