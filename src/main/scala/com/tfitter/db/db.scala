package com.tfitter.db

import org.joda.time.DateTime
import la.scala.util.Validated

object types {
  type UserID = Int
  type TwitID = Long
  type TwitCount = Int
  type FriendsCount = Int
  type UserFlags = Int
  type UTCOffset = Byte
  type TwitIDOpt = Option[TwitID]
}
import types._

// passed from JSON extractor to inserter
case class User (  
  uid: UserID,
  name: Option[String],
  screenName: String,
  statusesCount: Int, // declared, embedded in each status
  friendsCount: FriendsCount,
  time: DateTime,
  location: Option[String],
  utcOffset: Option[UTCOffset]
  )

// stored in the database
case class UserStats (
  val uid: UserID, // the only immutable
  var firstTwit: TwitID,
  var lastTwit: TwitID,
  var firstTwitTime: DateTime,
  var lastTwitTime: DateTime,
  var totalTwits: TwitCount,
  var totalTwitsDeclared: TwitCount,
  var numFriends: FriendsCount,
  var numReplyTwits: TwitCount,
  var numReplyUsers: TwitCount,
  var flags: Int
)

case class ReplyTwit (
  tid: TwitID,
  replyTwit: Option[TwitID],
  replyUser: UserID
  )
  
case class Twit (
  tid: TwitID,
  uid: UserID,
  time: DateTime,
  text: String,
  reply: Option[ReplyTwit]
  ) extends Validated {
  def isValid = reply match {
    case Some(r) => tid == r.tid
    case _ => true
  }
}

object Twit {
  def apply(t: Twit, r: ReplyTwit): Twit =
    Twit(t.tid,t.uid,t.time,t.text,
      Some(r))
}


case class UserTwit (
  user: User,
  twit: Twit
  )
  
case class TwitRange (
  first: TwitID,
  last:  TwitID,
  total: TwitCount
)

// User's twitter range, actual, declared, and with flags
case class UserTwitRange (
  range:       TwitRange,
  declared:    TwitCount,
  flags:       Int
)

// moving either range's end means 
// also adjusting the count
case class AdjustRange (
  twit: TwitID,
  time: DateTime,
  total: TwitCount
)

case class DBError(reason: String) extends Exception(reason)
case class DBEncoding(override val reason: String) extends DBError(reason)
case class DBDuplicate(override val reason: String) extends DBError(reason)

trait TwitterDB {

  // could replace object by a class with parameter UserID
  // and create short-lived objects per user

  // NBS originally had (uid: UserID) but the same after
  // class UserPG(uid: UserID) caused the new erreur,
  //error: using named or default arguments in a super constructor call is not allowed
  // and the caret pointing to UserPG...
  // can either cut the params here or there?
  
  abstract class UserDB(uid: UserID) {

    protected var stats: Option[UserStats] = None

    def exists: Boolean // as fetched

    def fetchStats: Option[UserStats] // without setting
    def fetch: Unit = stats = fetchStats
    def getStats: Option[UserStats] = stats
    def getFetchStats: Option[UserStats] = { 
      fetch
      getStats
    }
    
    // Constructor code: 
    // call filling stats from DB:
    fetch
    
    def storeStats(us: UserStats): Unit // without setting
    def store: Unit = stats match {
      case Some(x) => storeStats(x)
      case _ => 
    }
    def setStats(us: UserStats): Unit = stats = Some(us)
    def setStoreStats(us: UserStats): Unit = {
      setStats(us)
      storeStats(us)
    }

    // store adjusted range in the database
    def storeRangeFirst: Unit
    def storeRangeLast:  Unit
  
    def getFirst: Option[TwitID] = stats match {
      case Some(x) => Some(x.firstTwit)
      case _ => None
    }

    def getLast:  Option[TwitID] = stats match {
      case Some(x) => Some(x.lastTwit)
      case _ => None
    }

    def getTotalTwits: Option[TwitCount] = stats match {
      case Some(x) => Some(x.totalTwits)
      case _ => None
    }
    // could do more getters if needed

    // flags for the range status
    // we structure them so that `good` cases are all 0
    val Seq(retry, needsPast, pastUnreachable) = (0 to 2).map(1 << _)
    
    // def setFlags(i: UserFlags): Unit
    // def getFlags: Option[UserFlags]

    // absorb a new UserTwit record
    def updateUserForTwit(ut: UserTwit) = {
      val UserTwit(u,t) = ut
      val tid = t.tid

      val (numReplyTwitsAdd, numReplyUsersAdd) =
        t.reply match {
          case Some(x) => x.replyTwit match {
            case Some(_) => (1,0)
            case _ => (0,1)
          }
          case _ => (0,0)
        }

      stats match { 
        case Some(x) => { import x._
          totalTwits += 1
          totalTwitsDeclared = u.statusesCount
          numFriends = u.friendsCount
          numReplyTwits += numReplyTwitsAdd
          numReplyUsers += numReplyUsersAdd
          if (tid < firstTwit) {
            // err.println("user "+x.uid+" going backward at twit "+tid)
            firstTwit = tid
            firstTwitTime = t.time
            storeRangeFirst
          } else if (tid > lastTwit) {
            lastTwit = tid
            lastTwitTime = t.time
            storeRangeLast
          }
        }
        case _ => { 
          setStoreStats(
            UserStats(
              uid, tid, tid, t.time, t.time, 1,
              u.statusesCount, u.friendsCount,
              numReplyTwitsAdd, numReplyUsersAdd, 0
            )
          )
        }
      }
    }
  }

  // case class Duplicate(tid: TwitID) extends DBError

  abstract class TwitDB(tid: TwitID) {
    def exists: Boolean
    def isReply: Boolean

    def put(t: Twit): Unit // can raise Duplicate
    def getCore: Option[Twit]
    def getFull: Option[Twit]
    def getReply: Option[ReplyTwit]
  }

  case class SubParams (
    makeTwit:  TwitID => TwitDB, 
    makeUser:  UserID => UserDB,
    txnBegin:     () => Unit,
    txnCommit:    () => Unit,
    txnRollback:  () => Unit
    )
    
    
  // curry make/txn params
  def insertUserTwitCurry(subParams: SubParams)(ut: UserTwit): Unit = {
    import System.err
    
    val SubParams(makeTwit,makeUser,txnBegin,txnCommit,txnRollback) = subParams
    
    val UserTwit(user,twit) = ut
    val uid = user.uid
    val tid = twit.tid
    try {

      val t = makeTwit(tid) // TwitPG(tid)

      txnBegin
      t put twit // will cause exception if present and rollback
      val u = makeUser(uid) // UserPG(uid)
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

  // let's make a hard-code one too to compare
  def insertUserTwit(ut: UserTwit)

  def allUserStatsList:     List[UserStats]  
  def allUserStatsStream: Stream[UserStats]

  // pardon the pun  
  abstract class TwIterator extends Iterator[Twit]
  def allTwits: TwIterator  
}
