package com.tfitter.db.graph

import System.err
import db.types._
import com.tfitter.Repliers
import com.tfitter.Serialized.loadRepliers

import org.suffix.util.bdb.{BdbArgs,BdbFlags,BdbStore}

import com.sleepycat.persist.model.{Entity,PrimaryKey,SecondaryKey}
import com.sleepycat.persist.model.Relationship.MANY_TO_ONE

case class RepPair (
  s: UserID,
  t: UserID,
  reps: TwitCount,
  dirs: TwitCount
  )
   
@Entity
class RepPairBDB {
  @PrimaryKey
  var s: java.lang.Integer = null
  @SecondaryKey{val relate=MANY_TO_ONE}
  var t: java.lang.Integer = null
  var reps: java.lang.Integer = null
  var dirs: java.lang.Integer = null
  def this(_s: UserID, _t: UserID, _reps: TwitCount, _dirs: TwitCount) = { 
    this()
    s = _s
    t = _t
    reps = _reps
    dirs = _dirs
  }
  def this(rp: RepPair) = {
    this()
    s = rp.s
    t = rp.t
    reps = rp.reps
    dirs = rp.dirs
  }
  def toRepPair: RepPair = RepPair(s.intValue,t.intValue,reps.intValue,dirs.intValue)
}

class RepliersBDB(bdbArgs: BdbArgs) extends BdbStore(bdbArgs) {
  val rpPrimaryIndex =
    store.getPrimaryIndex(classOf[java.lang.Integer], classOf[RepPairBDB])

  val rpSecIndexTo =
      store.getSecondaryIndex(rpPrimaryIndex, classOf[java.lang.Integer], "t")
      
  def saveMap(r: Repliers, showProgress: Boolean): Unit = {
    var edgeCount = 0
    for ((s,st) <- r.reps; (t,(reps,dirs)) <- st) {
      val edge = new RepPairBDB(s,t,reps,dirs)
      rpPrimaryIndex.put(txn, edge)
      edgeCount += 1
      if (showProgress && edgeCount % 100000 == 0) err.print('.')
    }
    err.println
  }
  
  // def loadMap(showProgress: Boolean): Repliers = { 
  // }
}

object StoreRepliersBDB extends optional.Application {
  def main(
    envName: Option[String],
    storeName: Option[String],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],
    showProgress: Option[Boolean],
    args: Array[String]) = {
      
    val repSerName: String = args(0) // need it

    val bdbEnvPath   = envName getOrElse "reps.bdb"// Config.bdbEnvPath
    val bdbStoreName = storeName getOrElse "repliers"// Config.bdbStoreName
    val bdbCacheSize = cacheSize match {
      case Some(x) => Some((x*1024*1024*1024).toLong)
      case _ => None // Config.bdbCacheSize
    }
    val bdbFlags = BdbFlags(
      allowCreate   getOrElse false,
      readOnly      getOrElse false,
      transactional getOrElse false,
      deferredWrite getOrElse false,
      noSync        getOrElse false
    )
    val bdbArgs = BdbArgs(bdbEnvPath,bdbStoreName,bdbFlags,bdbCacheSize)

    // make this a parameter:
    val showingProgress = showProgress getOrElse false

    val repsDb = new RepliersBDB(bdbArgs)
    
    val reps: Repliers = loadRepliers(repSerName)
    
    err.print("Saving Repliers to Berkeley DB... ")
    repsDb.saveMap(reps,showingProgress)
    err.println("done")
  }
}
