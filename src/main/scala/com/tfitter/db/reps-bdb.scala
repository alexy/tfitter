package com.tfitter.db.graph

import sleepycat.persist.model._
import util.Sorting.stableSort
import System.err
import db.types._
import com.tfitter.Repliers
import com.tfitter.Serialized.loadRepliers

import org.suffix.util.bdb.{BdbArgs,BdbFlags,BdbStore}

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

@Persistent
class EdgeBDB {
  @KeyField(1) var s: JInt = null
  @KeyField(2) var t: JInt = null
  def this(_s: Int, _t: Int) = { this()
    s = _s
    t = _t
  }
}
   
@Entity
class RepPairBDB {
  // or can just shift two Ints into a Long
  @PrimaryKey
  var st: EdgeBDB = new EdgeBDB
  var reps: JInt = null
  var dirs: JInt = null
  def this(_s: UserID, _t: UserID, _reps: TwitCount, _dirs: TwitCount) = { this()
    st = new EdgeBDB(_s, _t)
    reps = _reps
    dirs = _dirs
  }
  def this(rp: RepPair) = { this()
    st = new EdgeBDB(rp.s,rp.t)
    reps = rp.reps
    dirs = rp.dirs
  }
  def toRepPair: RepPair = RepPair(st.s.intValue,st.t.intValue,reps.intValue,dirs.intValue)
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
  def repMapCacheStats: String
}


class RepliersBDB(bdbArgs: BdbArgs) extends BdbStore(bdbArgs) with RepsBDB {
  val rpPrimaryIndex =
    store.getPrimaryIndex(classOf[EdgeBDB], classOf[RepPairBDB])
      
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
    val curIter = new CursorIterator(rpPrimaryIndex.entities(
      new EdgeBDB(u, 0), true,
      new EdgeBDB(u, Math.MAX_INT), true))
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

  def repMapCacheStats: String = "cache not implemented for triples yet"
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

  var repMapCache: ReplierMap = UMap.empty
  var repMapCacheHits: Long   = 0
  var repMapCacheMisses: Long = 0
  
  def getReps(u: UserID): Option[RepCount] = {
    if (repMapCache contains u) {
      repMapCacheHits += 1
      Some(repMapCache(u))
    }
    else {
      repMapCacheMisses += 1      
      val uj = urPrimaryIndex.get(u)
      uj match {
        case null => None
        case ur => val reps = ur.toUserReps.rc
          repMapCache(u) = reps
          Some(reps)
      }
    }
  }

  def repMapCacheStats: String =
    "replier map hits: "+repMapCacheHits+", misses: "+repMapCacheMisses+
            ", total: "+(repMapCacheHits+repMapCacheMisses)
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
    
    // println("repliers for "+u1+udb.getReps(u1))
    // println("repliers for "+u2+udb.getReps(u2))
    
    import Communities._
    val c = new Communities(udb.getReps _)
    // the pairs in a different order yield
    // a different result set!
    List[UserPair](
    (25688017,14742479),(14742479,25688017),
    (30271870,26073346),(26073346,30271870),
    (12921202,29524566),(29524566,12921202))
    .foreach { up: UserPair =>
      val com: Community = c.triangles(up,Some(100),None)
      println("# "+up+" community:")
      println(c.showCommunity(com))
      println("# "+up+" fringe:")
      println(c.fringeUsers(com) mkString ",")
      println("# cache stats: "+udb.repMapCacheStats)
      println("#"+"-"*50)
    }
  }
} 

object Communities {
  import scala.collection.immutable.Queue
  type Gen = Int
  type Tie = Int
  type UserSet  = Set[UserID]
  type UserList = List[UserID]
  type UserPair = (UserID,UserID)
  type TiesPair = (Tie,Tie)
  type UserPairQueue = Queue[(UserPair,Gen)]
  type UserTies = (UserID,TiesPair)
  type ComMember = (UserID,UserPair,TiesPair,Gen)
  type Community = List[ComMember]
  type ComTroika = (UserPairQueue,UserSet,Community)
  type FringeUser = (UserPair, TiesPair)
  type Fringe = Set[FringeUser]
}

class Communities(getReps: UserID => Option[RepCount]) {
  import scala.collection.immutable.Queue
  import Communities._

  def triangles(up: UserPair,
    maxTotal: Option[Int], maxGen: Option[Int]): Community = {
    val (u1,u2) = up  
      
    def firstGreater(a: RePair,b: RePair) = a._1 > b._1
  
    def common(a: List[RePair], b: Array[RePair], haveSet: UserSet): Option[UserTies] = {
      def aux(l: List[RePair]): Option[UserTies] = l match {
        case (s,(t1,u))::xs => if (haveSet contains s) aux(xs)
        // We rely on Array a.find finding leftmost item here, 
        // so it is the highest rank -- must confirm from Scala source!
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
      stableSort(u2a,firstGreater _)
    
      val c: Option[UserTies] = common(u1a, u2a, haveSet)
      // err.println(c)
      // val s = Set(1,2,3)
      // s flatMap (x => List((x,x),(x,x+1)))
      
      c match {
        case Some((x,ties)) => 
          val cGen = parentGen + 1
          val newQueue = 
            deQueue enqueue (community map { case (y,_,_,gen) => 
              ((y,x),gen min cGen) }) // min or max?
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


  def fringeUsers(com: Community): UserSet = {
    // 2.7 has no list.toSet, mharrah suggest either:
    // Set(list:_*) or Set()++list
    val comSet: UserSet = scala.collection.immutable.Set(com:_*) map (_._1)

    // TODO how can we employ flatMap to simplify the below in 2.7?
    // in 2.8, we can use filterMap. nonEmpty, and whatnot...
    var fringeUsers = (comSet map getReps) filter (!_.isEmpty) map (Set()++_.get.keySet) map (_ -- comSet) reduceLeft (_++_)
      
    fringeUsers
  }

  def showCommunity(com: Community): String = {
    val comSet: UserSet = scala.collection.immutable.Set(com:_*) map (_._1)

    // fun implementing mkString ourselves:
    // s.foldLeft (""){ case (x:String,i:Int) if !x.isEmpty => x+","+i; case (_,i) => i.toString }
    // s.reduceLeft( (r:Any,i:Int) => r + "," + i ).toString
    comSet mkString ","
  }
}
