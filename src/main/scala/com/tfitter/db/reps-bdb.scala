package com.tfitter.db.graph

import util.Sorting.stableSort
import System.err
import db.types._
import com.tfitter.Repliers
import com.tfitter.Serialized.loadRepliers

import org.suffix.util.bdb.{BdbArgs,BdbFlags,BdbStore}

import com.sleepycat.persist.model.{Entity,PrimaryKey,SecondaryKey,Persistent}
import com.sleepycat.persist.model.Relationship.MANY_TO_ONE
import scala.collection.mutable.{Map=>UMap}

import java.lang.{Integer=>JInt,Long=>JLong}
import java.util.{HashMap=>JHMap}

object types {
  type RePair = (UserID,(TwitCount,TwitCount))
  type RepCount = UMap[UserID,(TwitCount,TwitCount)]
  type ReplierMap = UMap[Int,RepCount]
  type FixedRepliers = Map[UserID, Map[UserID,(TwitCount,TwitCount)]]
}
import types._


case class RepPair (
  s: UserID,
  t: UserID,
  reps: TwitCount,
  dirs: TwitCount
  )
  
case class UserReps (
  s: UserID,
  rc: RepCount
  )
   
@Entity
class RepPairBDB {
  // or can just shift two Ints into a Long
  @PrimaryKey
  // var st: java.util.AbstractMap.SimpleImmutableEntry[java.lang.Integer,java.lang.Integer] = null
  var st: JLong = null
  @SecondaryKey{val relate=MANY_TO_ONE}
  var s: JInt = null
  @SecondaryKey{val relate=MANY_TO_ONE}
  var t: JInt = null
  var reps: JInt = null
  var dirs: JInt = null
  def this(_s: UserID, _t: UserID, _reps: TwitCount, _dirs: TwitCount) = { 
    this()
    // st = new java.util.AbstractMap.SimpleImmutableEntry(_s, _t)
    st = _s << 32L | _t
    s = _s
    t = _t
    reps = _reps
    dirs = _dirs
  }
  def this(rp: RepPair) = {
    this()
    // st = new java.util.AbstractMap.SimpleImmutableEntry(rp.s, rp.t)
    st = rp.s << 32L | rp.t
    s = rp.s
    t = rp.t
    reps = rp.reps
    dirs = rp.dirs
  }
  def toRepPair: RepPair = RepPair(s.intValue,t.intValue,reps.intValue,dirs.intValue)
}

@Entity
class UserRepsBDB {
  @PrimaryKey
  var s: java.lang.Integer = null
  var t: JHMap[JInt,JInt] = null
  var u: JHMap[JInt,JInt] = null
  def this(_s: UserID, rc: RepCount) = { this()
  // Map(1->(2,3),4->(5,2),2->(7,8)).foldLeft(Nil:List[(Int,Int)],Nil:List[(Int,Int)]) { 
  // case ((acc1, acc2), (x, (y, z))) => ( (x,y)::acc1, (x,z)::acc2) }  
    s = _s
    t = new JHMap
    u = new JHMap
    rc.foreach { case (x, (y, z)) => t.put(x,y); u.put(x,z) }    
  }
  def toUserReps: UserReps = {
    val sid = s.intValue
    var m: RepCount = UMap.empty
    val tit = t.entrySet.iterator
    val uit = u.entrySet.iterator
    while (tit.hasNext) {
      val tp = tit.next
      val up = uit.next
      val s = tp.getKey
      assert (up.getKey == s)
      val t = tp.getValue
      val u = up.getValue
      // m += (s -> (t,u))
      m(s.intValue) = (t.intValue,u.intValue)
    }
    UserReps(sid,m)
  }
}


trait RepsBDB {
  def saveMap(r: Repliers, showProgress: Boolean): Unit
  def loadMap(showProgress: Boolean): ReplierMap
  def getReps(u: UserID): Option[RepCount]
}


class RepliersBDB(bdbArgs: BdbArgs) extends BdbStore(bdbArgs) with RepsBDB {
  val rpPrimaryIndex =
    store.getPrimaryIndex(classOf[java.lang.Long], classOf[RepPairBDB])

  val rpSecIndexFrom =
      store.getSecondaryIndex(rpPrimaryIndex, classOf[java.lang.Integer], "s")

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
  
  def loadMap(showProgress: Boolean): ReplierMap = { 
    val curIter = new CursorIterator(rpPrimaryIndex.entities)
    var reps: FixedRepliers = Map.empty

    var edgeCount = 0
    for (ej <- curIter) {
      val e: RepPair = ej.toRepPair
      if (reps.contains(e.s)) reps(e.s)(e.t) = (e.reps,e.dirs)
      // = Map(x->y) is harder to do with UMap,
      // hence FixedRepliers -- and immutable at that!
      // alas, we have to cast back to mutable ReplierMap
      // to fit the trait RepsBDB which also fits RepMaps
      else reps(e.s) = Map(e.t -> (e.reps,e.dirs))
      edgeCount += 1
      if (showProgress && edgeCount % 100000 == 0) err.print('.')
    }
    err.println
    reps.asInstanceOf[ReplierMap]
  }
  
  def getReps(u: UserID): Option[RepCount] = {
    val curIter = new CursorIterator(rpSecIndexFrom.subIndex(u).entities)
    val rc: RepCount = UMap.empty
    
    for (ej <- curIter) {
      val e: RepPair = ej.toRepPair
      assert(e.s == u)
      rc(e.t) = (e.reps,e.dirs)
    }
    // == UMap.empty or count edges inserted vs 0:
    if (rc == UMap.empty) None
    else Some(rc)
  }
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
    val showingProgress = showProgress getOrElse true

    val repsDb = new RepliersBDB(bdbArgs)
    
    val reps: Repliers = loadRepliers(repSerName)
    
    err.print("Saving Repliers to Berkeley DB... ")
    repsDb.saveMap(reps,showingProgress)
    err.println("done")
  }
}


object FetchRepliersBDB extends optional.Application {
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
      

    val bdbEnvPath   = envName getOrElse "reps.bdb"// Config.bdbEnvPath
    val bdbStoreName = storeName getOrElse "repliers"// Config.bdbStoreName
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

    // make this a parameter:
    val showingProgress = showProgress getOrElse true

    val repsDb = new RepliersBDB(bdbArgs)
    
    err.print("Loading Repliers from Berkeley DB... ")
    val reps: ReplierMap = repsDb.loadMap(showingProgress)
    err.println("done")
  }
}


class RepMapsBDB(bdbArgs: BdbArgs) extends BdbStore(bdbArgs) with RepsBDB {
  val urPrimaryIndex =
    store.getPrimaryIndex(classOf[JInt], classOf[UserRepsBDB])
      
  def saveMap(r: Repliers, showProgress: Boolean): Unit = {
    var userCount = 0
    for ((s,rc) <- r.reps) {
      val userReps = new UserRepsBDB(s,rc)
      urPrimaryIndex.put(txn, userReps)
      userCount += 1
      if (showProgress && userCount % 100000 == 0) err.print('.')
    }
    err.println
  }
  
  def loadMap(showProgress: Boolean): ReplierMap = { 
    val curIter = new CursorIterator(urPrimaryIndex.entities)
    var reps: ReplierMap = UMap.empty

    var userCount = 0
    for (uj <- curIter) {
      val ur: UserReps = uj.toUserReps
      reps(ur.s) = ur.rc
      userCount += 1
      if (showProgress && userCount % 100000 == 0) err.print('.')
    }
    err.println
    reps
  }
  
  def getReps(u: UserID): Option[RepCount] = {
    val uj = urPrimaryIndex.get(u)
    uj match {
      case null => None
      case u => Some(u.toUserReps.rc)
    }
  }
}


object StoreUserRepsBDB extends optional.Application {
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

    val bdbEnvPath   = envName getOrElse "urs.bdb"// Config.bdbEnvPath
    val bdbStoreName = storeName getOrElse "repmaps"// Config.bdbStoreName
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
    val showingProgress = showProgress getOrElse true

    val ursDb = new RepMapsBDB(bdbArgs)
    
    val reps: Repliers = loadRepliers(repSerName)
    
    err.print("Saving Repliers to Berkeley DB... ")
    ursDb.saveMap(reps,showingProgress)
    err.println("done")
  }
}


object FetchUserRepsBDB extends optional.Application {
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
      

    val bdbEnvPath   = envName getOrElse "urs.bdb"// Config.bdbEnvPath
    val bdbStoreName = storeName getOrElse "repmaps"// Config.bdbStoreName
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

    // make this a parameter:
    val showingProgress = showProgress getOrElse true

    val ursDb = new RepMapsBDB(bdbArgs)
    
    err.print("Loading Repliers from Berkeley DB... ")
    val reps: ReplierMap = ursDb.loadMap(showingProgress)
    err.println("done")
  }
}


// this doesn't work -- we need a PersistentProxy
// for Scala HashMap
@Persistent
class PerRepCount {
  var rc: RepCount = UMap.empty
  def set(_rc: RepCount) = rc = _rc
  def get: RepCount = rc
}

@Entity
class ScalaRepsBDB {
  @PrimaryKey
  var s: JInt = 0
  var rc: PerRepCount = new PerRepCount
  def this(_s: UserID, _rc: RepCount) = { this()
    s = _s
    rc.set(_rc)
  }
}

class ScalaMapsBDB(bdbArgs: BdbArgs) extends BdbStore(bdbArgs) {
  val urPrimaryIndex =
// Caused by: java.lang.IllegalArgumentException: Wrong primary key class: int Correct class is: java.lang.Integer  
    store.getPrimaryIndex(classOf[JInt], classOf[ScalaRepsBDB])
      
  def saveMap(r: Repliers, showProgress: Boolean): Unit = {
    var userCount = 0
    for ((s,rc) <- r.reps) {
      val userReps = new ScalaRepsBDB(s,rc)
// Caused by: java.lang.IllegalArgumentException: Class could not be loaded or is not persistent: scala.collection.mutable.HashMap
      urPrimaryIndex.put(txn, userReps)
      userCount += 1
      if (showProgress && userCount % 100000 == 0) err.print('.')
    }
    err.println
  }
  
  def loadMap(showProgress: Boolean): ReplierMap = { 
    val curIter = new CursorIterator(urPrimaryIndex.entities)
    var reps: ReplierMap = UMap.empty

    var userCount = 0
    for (ur <- curIter) {
      reps(ur.s.intValue) = ur.rc.get
      userCount += 1
      if (showProgress && userCount % 100000 == 0) err.print('.')
    }
    err.println
    reps
  }
}


object StoreScalaRepsBDB extends optional.Application {
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

    val bdbEnvPath   = envName   getOrElse "ursc.bdb"// Config.bdbEnvPath
    val bdbStoreName = storeName getOrElse "scamaps"// Config.bdbStoreName
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
    val showingProgress = showProgress getOrElse true

    val urscDb = new ScalaMapsBDB(bdbArgs)
    
    val reps: Repliers = loadRepliers(repSerName)
    
    err.print("Saving Repliers to Berkeley DB... ")
    urscDb.saveMap(reps,showingProgress)
    err.println("done")
  }
}


object PairsBDB extends optional.Application {
 
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
    usePairs: Option[Boolean],
    args: Array[String]) = {
      
    val usingPairs = usePairs getOrElse false
    
    val bdbEnvPath   = envName getOrElse (if (usingPairs) "reps.bdb" else "urs.bdb")
    val bdbStoreName = storeName getOrElse (if (usingPairs) "repliers" else "repmaps")
    
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

    // make this a parameter:
    val showingProgress = showProgress getOrElse true

    val udb = if (usingPairs) new RepliersBDB(bdbArgs)
      else new RepMapsBDB(bdbArgs)
      
    err.println("udb class: "+udb.getClass)

    val u1: UserID = 29524566 // Just_toddy
    
    val u2: UserID = 12921202 // myleswillsaveus
    
    println(udb.getReps(u1))
    println(udb.getReps(u2))
    
    import Communities._
    List[UserPair]((29524566,12921202),
    (25688017,14742479),(14742479,25688017),(30271870,26073346),
    (26073346,30271870),(12921202,29524566),(29524566,12921202))
    .foreach { up: UserPair =>
      val com: Community = triangles(up,udb.getReps _,Some(100),None)
      println(com)
      println("-"*50)
    }
  }
} 

object Communities {
  import scala.collection.immutable.Queue
  
  type Gen = Int
  type Tie = Int
  type UserPair = (UserID,UserID)
  type TiesPair = (Tie,Tie)
  type UserPairQueue = Queue[(UserPair,Gen)]
  type UserSet = Set[UserID]
  type UserTies = (UserID,TiesPair)
  type ComMember = (UserID,UserPair,TiesPair,Gen)
  type Community = List[ComMember]
  type ComTroika = (UserPairQueue,UserSet,Community)
  
  def triangles(up: UserPair, getReps: UserID => Option[RepCount],
    maxTotal: Option[Int], maxGen: Option[Int]): Community = {
    val (u1,u2) = up  
      
    def repairGreater(a: RePair,b: RePair) = a._1 > b._1
  
    def common(a: List[RePair], b: Array[RePair], haveSet: UserSet): Option[UserTies] = {
      def aux(l: List[RePair]): Option[UserTies] = l match {
        case (s,(t1,u))::xs => if (haveSet contains s) aux(xs)
        else b.find(_._1==s) match {
          case Some((_,(t2,_))) => Some((s,(t1,t2)))
          case _ => aux(xs)
        }
        case _ => None
      }
      aux(a)
    }
    
    def addPair(troika: ComTroika): (ComTroika,Boolean) = {
      val (pairs,haveSet,community) = troika
      if (pairs.isEmpty) return (troika,false)
      
      val ((parents @ (u1,u2),parentGen),deQueue) = pairs.dequeue
      // println("dequeued "+parents)
      
      // can we unify return deQueue...true into a single point or single value?
      
      if (!maxGen.isEmpty && parentGen >= maxGen.get)   return ((deQueue,haveSet,community),true)
      // need { return ... } in a block, or illegal start of a simple expression:
      val u1reps: RepCount = getReps(u1) getOrElse    { return ((deQueue,haveSet,community),true) }
      // println(u1reps)
      val u2reps: RepCount = getReps(u2) getOrElse    { return ((deQueue,haveSet,community),true) }
      // println(u2reps)
    
      val u1a = u1reps.toList.sort(_._2._1 > _._2._1)
      val u2a = u2reps.toArray
      stableSort(u2a,repairGreater _)
    
      val c: Option[UserTies] = common(u1a, u2a, haveSet)
      // err.println(c)
      // val s = Set(1,2,3)
      // s flatMap (x => List((x,x),(x,x+1)))
      
      c match {
        case Some((x,ties)) => 
          val cGen = parentGen + 1
          val newQueue = 
            deQueue enqueue (community map { case (y,_,_,gen) => 
              ((y,x),gen min cGen) })
          // println("new queue: "+newQueue)
          val keepGoing = maxTotal.isEmpty || haveSet.size + 1 < maxTotal.get
          ((newQueue,haveSet+x,(x,parents,ties,cGen)::community),keepGoing)
        case _ => ((deQueue,haveSet,community),true)
      }
    }
    
    def growCommunity(troika: ComTroika): Community =
      addPair(troika) match {
        case (t,true) => growCommunity(t)
        case ((_,_,com),_) => com.reverse
      }
    
    // body!
    val pq: UserPairQueue = Queue.Empty.enqueue(((u1,u2),0))
    growCommunity( ( pq, Set(u1,u2), List[ComMember]((u1,(0,0),(0,0),0),(u2,(0,0),(0,0),0)) ) )
  }
}
