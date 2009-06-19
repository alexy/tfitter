package la.scala.twitter.db

abstract trait TwitDB {
  type UserID = Int
  type TwitID = Long
  type TwitCount = Int
  type UserFlags = Int
  
  class DBError extends Exception
  
  // need case here and below
  // to make parameters publicly dot.accessible!
  case class TwitRange (
    first: TwitID,
    last:  TwitID,
    total: TwitCount
  )
  
  // User's twitter range, actual, declared, and with flags
  case class UTRange (
    range:       TwitRange,
    declared:    TwitCount,
    flags:       Int
  )

  // moving either range's end means 
  // also adjusting the count
  case class AdjustRange (
    endpoint: TwitID, 
    total: TwitCount
  )

  // could replace object by a class with parameter UserID
  // and create short-lived objects per user

  // NBS originally had (uid: UserID) but the same after
  // class UserPG(uid: UserID) caused the new erreur,
  //error: using named or default arguments in a super constructor call is not allowed
  // and the caret pointing to UserPG...
  // can either cut the params here or there?
  
  abstract class User(uid: UserID) {
    
    // flags for the range status
    // we structure them so that `good` cases are all 0
    
    val Seq(retry, needsPast, pastUnreachable) = (0 to 2).map(1 << _)
          
    // set/get user range as a whole
    def uRange_=(r: UTRange): Unit
    def uRange: Option[UTRange]
    def range: Option[TwitRange]
  
    // adjust range
    def rangeFirst_=(ar: AdjustRange)
    def rangeLast_=(ar:  AdjustRange)
  
    def rangeFirst: Option[TwitID]
    def rangeLast:  Option[TwitID]
    def totalTwits: Option[TwitCount]
  
    def declaredTwits_=(d: TwitCount): Unit
    def declaredTwits: Option[TwitCount]
  
    // ideally, we'd change flags by individual defs,
    // one by one, then they'd be flushed to disk
    // along with any other changes -- can be done
    // in a flush method called by the creator of 
    // this object
  
  /*  
    def flags_=(i: UserFlags): Unit
    def flags: Option[UserFlags]

    def retry_=(b: Boolean): Unit
    def retry: Boolean
    
    def needsPast_=(b: Boolean): Unit
    def needsPast: Boolean
    
    def pastUnreachable_=(b: Boolean): Unit
    def pastUnreachable: Boolean
  */
    // def retryUser :Boolean
    // def retryUser_=(flag: Boolean) :Unit
  }

  case class Twit (
    uid: UserID,
    tid: TwitID,
    text: String
    )
    
  class Duplicate(tid: TwitID) extends DBError
  
  /*
  def twitPut(t: Twit): Unit // can raise Duplicate
  
  def twitGet(tid: TwitID): Twit
  
  def haveTwit(tid: TwitID): Boolean
  */
}

//trait Nonames {
//  def f(Int) :Unit
//}
