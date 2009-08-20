package com.tfitter.db

import org.suffix.util.bdb.{BdbArgs,BdbFlags,BdbStore}

import System.err
import com.sleepycat.persist.model.{Entity,PrimaryKey,SecondaryKey}
import com.sleepycat.persist.{EntityCursor}
import com.sleepycat.persist.model.Relationship.MANY_TO_ONE
import org.joda.time.DateTime

import types._
// cannot extend UserStats due to vars 
// and no need for extra fields in the datastore

@Entity
class UserStatsBDB {
  @PrimaryKey
  var uid: java.lang.Integer = null
  var firstTwit: java.lang.Long = null
  var lastTwit: java.lang.Long = null
  // Exception in thread "main" java.lang.IllegalArgumentException:
  // Class could not be loaded or is not persistent: org.joda.time.DateTime
  var firstTwitTime: java.util.Date = null
  var lastTwitTime: java.util.Date = null
  var totalTwits: java.lang.Integer = null 
  var totalTwitsDeclared: java.lang.Integer = null
  var numFriends: java.lang.Integer = null
  var numReplyTwits: java.lang.Integer = null
  var numReplyUsers: java.lang.Integer = null
  var flags: java.lang.Integer = null
  
  def this(
    _uid: UserID,
    _firstTwit: TwitID,
    _lastTwit: TwitID,
    _firstTwitTime: DateTime,
    _lastTwitTime: DateTime,
    _totalTwits: TwitCount,
    _totalTwitsDeclared: TwitCount,
    _numFriends: FriendsCount,
    _numReplyTwits: TwitCount,
    _numReplyUsers: TwitCount,
    _flags: Int) = { this()
    uid = _uid
    firstTwit= _firstTwit
    lastTwit = _lastTwit
    firstTwitTime = _firstTwitTime.toDate
    lastTwitTime = _lastTwitTime.toDate
    totalTwits = _totalTwits
    totalTwitsDeclared = _totalTwitsDeclared
    numFriends = _numFriends
    numReplyTwits = _numReplyTwits
    numReplyUsers = _numReplyUsers
    flags = _flags    
  }
  def this(u: UserStats) = this(
      u.uid,u.firstTwit,u.lastTwit,u.firstTwitTime,u.lastTwitTime,
      u.totalTwits,u.totalTwitsDeclared,u.numFriends,
      u.numReplyTwits,u.numReplyUsers,u.flags)       
  def toUserStats: UserStats = UserStats(
    uid.intValue,firstTwit.longValue,lastTwit.longValue,
    new DateTime(firstTwitTime),new DateTime(lastTwitTime),
    totalTwits.intValue,totalTwitsDeclared.intValue,numFriends.intValue,
    numReplyTwits.intValue,numReplyUsers.intValue,flags.intValue)

  override def toString: String = 
    "User uid:%d A-tid:%d Z-tid:%d A@:%Tc Z@:%Tc #t:%d t?:%d f:%d rt:%d ru:%d" format (
      uid,firstTwit,lastTwit,firstTwitTime,lastTwitTime,
      totalTwits,totalTwitsDeclared,numFriends,
      numReplyTwits,numReplyUsers,flags)
}


@Entity
class ReplyTwitBDB {
  @PrimaryKey
  var tid: java.lang.Long = null
  @SecondaryKey{val relate=MANY_TO_ONE}
  var replyTwit: java.lang.Long = null
  @SecondaryKey{val relate=MANY_TO_ONE}
  var replyUser: java.lang.Integer = null

  def this(
    _tid: TwitID,
    _replyTwit: Option[TwitID],
    _replyUser: UserID
    ) = { this()
      tid = _tid
      replyTwit = _replyTwit match {
        case Some(x) => x
        case _ => null
      }
      replyUser = _replyUser
    }
  def this(t: ReplyTwit) = this(t.tid,t.replyTwit,t.replyUser)
  def toReplyTwit: ReplyTwit = {
    val replyTwitLongOpt = replyTwit match {
      case null => None
      case x => Some(x.longValue)
    }
    ReplyTwit(tid.longValue,replyTwitLongOpt,replyUser.intValue)
  }

  override def toString:String = {
    var s = "Reply tid:%d ru:%d" format (tid,replyUser)
    if (replyTwit != null) s += " rt:"+replyTwit
    s
  }

}
  
@Entity
class TwitStoreBDB {
  @PrimaryKey
  var tid: java.lang.Long = null
  @SecondaryKey{val relate=MANY_TO_ONE}
  var uid: java.lang.Integer = null
  // in order to add this secondary index,
  // we need to evolve the class in BDB parlance
  // and assign a higher version
  // @SecondaryKey{val relate=MANY_TO_ONE}
  var time: java.util.Date = null // DateTime not persistent
  var text: String = null
  @SecondaryKey{val relate=MANY_TO_ONE}
  var replyTwit: java.lang.Long = null
  @SecondaryKey{val relate=MANY_TO_ONE}
  var replyUser: java.lang.Integer = null

  def this(
    _tid: TwitID,
    _uid: UserID,
    _time: DateTime,
    _text: String,
    _replyTwit: Option[TwitID],
    _replyUser: Option[UserID]
    ) = { this()
      tid = _tid
      uid = _uid
      time = _time.toDate
      text = _text
      replyUser = _replyUser match {
        case Some(user) => 
          replyTwit = _replyTwit match {
            case Some(twit) => twit
            case _ => null
          }
          user
        case _ => 
          replyTwit = null
          null
      }
    }
  // since we have to have this first thing,
  // we unroll t.reply match twice; better way?
  def this(t:Twit) =
    this(t.tid,t.uid,t.time,t.text,
      t.reply match {
        case Some(r) => r.replyTwit
        case _ => None
      },
      t.reply match {
        case Some(r) => Some(r.replyUser)
        case _ => None
      })
    
  def toTwit: Twit = {
    val reply = (replyUser,replyTwit) match {
      case (null,null) => None
      case (user,null) => Some(ReplyTwit(
          tid.longValue,None,user.intValue))
      case (user,twit) =>
        Some(ReplyTwit(
          tid.longValue,Some(twit.longValue),user.intValue))
    }
    Twit(tid.longValue,uid.intValue,new DateTime(time),text,reply)
  }

  override def toString:String = {
    var s = "Twit tid:%d uid:%d @:%Tc [%s]" format (tid,uid,time,text)
    if (replyUser != null) s += " ru:"+replyUser
    if (replyTwit != null) s += " rt:"+replyTwit
    s
  }
}


  
class TwitterBDB(bdbArgs: BdbArgs) extends BdbStore(bdbArgs) with TwitterDB {

  val userPrimaryIndex =
      store.getPrimaryIndex(classOf[java.lang.Integer], classOf[UserStatsBDB])

  val twitPrimaryIndex =
      store.getPrimaryIndex(classOf[java.lang.Long], classOf[TwitStoreBDB])

  val twitSecIndexUser =
      store.getSecondaryIndex(twitPrimaryIndex, classOf[java.lang.Integer], "uid")

  val twitSecIndexReplyTwit =
      store.getSecondaryIndex(twitPrimaryIndex, classOf[java.lang.Long], "replyTwit")

  val twitSecIndexReplyUser =
      store.getSecondaryIndex(twitPrimaryIndex, classOf[java.lang.Integer], "replyUser")

  val replyPrimaryIndex =
      store.getPrimaryIndex(classOf[java.lang.Long], classOf[ReplyTwitBDB])

  val replySecIndexTwit =
      store.getSecondaryIndex(twitPrimaryIndex, classOf[java.lang.Long], "replyTwit")
      
  val replySecIndexUser =
      store.getSecondaryIndex(twitPrimaryIndex, classOf[java.lang.Integer], "replyUser")
  

  case class UserBDB(uid: UserID) extends UserDB(uid: UserID) {

    // http://www.oracle.com/technology/documentation/berkeley-db/je/java/com/sleepycat/persist/PrimaryIndex.html#contains(K)
    // or use: contains(txn, key, lockMode)
    def exists: Boolean = userPrimaryIndex.contains(uid)
    
    def storeStats(us: UserStats): Unit = {
      val x = new UserStatsBDB(us)      
      userPrimaryIndex.put(txn,x)
    } 
    
    def fetchStats: Option[UserStats] =
      userPrimaryIndex.get(uid) match {
        // or does it throw a not found exception instead?
        case null => None 
        case x => Some(x.toUserStats)
      }
   
    def storeRangeFirst: Unit = store
    def storeRangeLast:  Unit = store
  }
  
  
  case class TwitBDB(tid: TwitID) extends TwitDB(tid: TwitID) {
    // or use: contains(txn, key, lockMode)
    def exists: Boolean = twitPrimaryIndex.contains(tid)
    def isReply: Boolean =replyPrimaryIndex.contains(tid)

    def put(t: Twit): Unit = {
      twitPrimaryIndex.put(txn, new TwitStoreBDB(t))
      t.reply match {
        case Some(r) => replyPrimaryIndex.put(txn, new ReplyTwitBDB(r))
        case _ =>
      }
    }
    
    def getCore: Option[Twit] = { 
      twitPrimaryIndex.get(tid) match {
        case null => None
        case t => Some(t.toTwit)
      }
    }

    def getFull: Option[Twit] = getCore
    
    def getReply: Option[ReplyTwit] =
      replyPrimaryIndex.get(tid) match {
        case null => None
        case r => Some(r.toReplyTwit)
      }
  }
  
  def insertUserTwit(ut: UserTwit): Unit = {
    import System.err
    
    val UserTwit(user,twit) = ut
    val uid = user.uid
    val tid = twit.tid
    try {

      val t = TwitBDB(tid)

      // txnBegin
      t put twit // will cause exception if present and rollback
      val u = UserBDB(uid)
      u.updateUserForTwit(ut)
      txnCommit
    } catch {
      case e => {
        err.println(e)
        err.println("ROLLBACK uid="+uid+" tid="+tid)
        txnRollback
      }
    }
  }



  def showData = {
  /* Iterate entities in primary and secondary key order. */
    println("--- Iterate Users by primary key ---")
    printAll(userPrimaryIndex.entities)
    println("--- Iterate Twits by primary key ---")
    printAll(twitPrimaryIndex.entities())
    println("--- Iterate Replies by primary key ---")
    printAll(replyPrimaryIndex.entities)
    // println("--- Iterate by secondary key ---")
    // printAll(secIndex.entities())
  }
  
  val subParams = SubParams(TwitBDB,UserBDB,txnBegin _,txnCommit _,txnRollback _)
  def insertUserTwitB = insertUserTwitCurry(subParams)(_)

  def allUserStatsList:     List[UserStats] = cursorMap(userPrimaryIndex.entities)(_.toUserStats)
  def allUserStatsStream: Stream[UserStats] = cursorStream(userPrimaryIndex.entities) map (_.toUserStats)

  // TODO lookup a general way to adapt a Java Iterator to Scala

  class TwIteratorBDB(cursor: EntityCursor[TwitStoreBDB]) extends TwIterator {
    val javaIter = cursor.iterator
    // TODO a way to stick closing actions into our iterator,
    // are there any official or better ways?
    def hasNext: Boolean = try { val has = javaIter.hasNext
      if (!has) cursor.close  
      has
    } catch { case _ => false }
    def next: Twit = javaIter.next.toTwit
  }

  def allTwits:  TwIterator = new TwIteratorBDB(twitPrimaryIndex.entities)
  def userTwits(uid: UserID): TwIterator = new TwIteratorBDB(twitSecIndexUser.subIndex(uid).entities)
}