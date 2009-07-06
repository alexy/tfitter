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
}
import types._

// passed from JSON extractor to inserter
case class User (  
  uid: UserID,
  name: String,
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

trait TwitterDB {

  // could replace object by a class with parameter UserID
  // and create short-lived objects per user

  // NBS originally had (uid: UserID) but the same after
  // class UserPG(uid: UserID) caused the new erreur,
  //error: using named or default arguments in a super constructor call is not allowed
  // and the caret pointing to UserPG...
  // can either cut the params here or there?
  
  abstract class UserDB(uid: UserID) {

    def exists: Boolean

    var stats: Option[UserStats] = None // var
    def getStats: Option[UserStats]
    def get // sets stats in the database
    // call filling stats from DB:
    get // stats = getStats
    
    // write all fields back
    def setStats(us: UserStats)
    def set // from stats in the database

    // adjust range
    def setRangeFirst
    def setRangeLast
  
    def getFirst:      Option[TwitID]
    def getLast:       Option[TwitID]
    def getTotalTwits: Option[TwitCount]
    // could do more getters if needed

    // flags for the range status
    // we structure them so that `good` cases are all 0
    val Seq(retry, needsPast, pastUnreachable) = (0 to 2).map(1 << _)
    def setFlags(i: UserFlags): Unit
    def getFlags: Option[UserFlags]

    // absorb a new UserTwit record
    def updateUserForTwit(ut: UserTwit)


  /*
    def retry_=(b: Boolean): Unit
    def retry: Boolean
    
    def needsPast_=(b: Boolean): Unit
    def needsPast: Boolean
    
    def pastUnreachable_=(b: Boolean): Unit
    def pastUnreachable: Boolean
  */
  }

  // case class Duplicate(tid: TwitID) extends DBError

  abstract class TwitDB(tid: TwitID) {
    def exists: Boolean
    def isReply: Boolean

    def put(t: Twit): Unit // can raise Duplicate
    def getCore: Option[Twit] // can raise NotFound
    def getFull: Option[Twit] // can raise NotFound
  }
  
  def insertUserTwit(ut: UserTwit)

}

//trait Nonames {
//  def f(Int) :Unit
//}

// user profile:
// last time if tweet
// total number of replies to twits
// -''- to users without twits