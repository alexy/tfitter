package com.tfitter.db


import org.joda.time.DateTime

class TwitterPG(jdbcURL: String, user: String, pwd: String,
             rangeTable: String, twitTable: String, replyTable: String) extends TwitterDB {
  import types._

  import java.sql.{DriverManager, Connection, ResultSet, PreparedStatement, Statement, Date}
  import DriverManager.{getConnection => connect}
  
  import org.suffix.util.FunctionNotation._

  val rtUser     = "uid"
  val rtFirst    = "first"
  val rtLast     = "last"
  val rtTotal    = "total"
  val rtDeclared = "declared"
  val rtFlags    = "flags"

  import la.scala.sql.rich.RichSQL._

  implicit val conn = connect(jdbcURL, user, pwd)

  def commaSeparated(s: String, n: Int): String = 
    /*List.fill(n)(s) */ List.make(n,s) mkString "," // 2.8/7
    
  def insertFmt(n: Int): String  = "insert into %s ("+commaSeparated(
    "%s",n)+") values ("+commaSeparated("?",n)+")"

  def updateFmt(n: Int): String = {
    val seqwhat = "%s=?"
    "update %s set "+commaSeparated(
    seqwhat,n)+" where "+seqwhat
   }

  def selectFmt(n: Int): String =
    "select "+commaSeparated("%s",n)+" from %s where %s = ?"

  def selectCountFmt : String =
    "select count(*) from %s where %s = ?"

  val insertRangeFullSt = conn prepareStatement (
    insertFmt(6)
    format (rangeTable,rtUser,rtFirst,rtLast,rtTotal,rtDeclared,rtFlags)
    )

  val selectRangeFullSt = conn prepareStatement (
    selectFmt(5)
    format (rtFirst,rtLast,rtTotal,rtDeclared,rtFlags,rangeTable,rtUser)
    )

  val selectRangeSt = conn prepareStatement (
    selectFmt(3)
    format (rtFirst,rtLast,rtTotal,rangeTable,rtUser)
    )

  val updateRangeFirstSt = conn prepareStatement (
    updateFmt(2)
    format (rangeTable,rtFirst,rtTotal,rtUser)
    )

  val updateRangeLastSt = conn prepareStatement (
    updateFmt(2)
    format (rangeTable,rtLast,rtTotal,rtUser)
    )

  val selectRangeFirstSt = conn prepareStatement (
    selectFmt(1)
    format (rtFirst,rangeTable,rtUser)
  )

  val selectRangeLastSt = conn prepareStatement (
    selectFmt(1)
    format (rtLast,rangeTable,rtUser)
  )

  val selectRangeTotalSt = conn prepareStatement (
    selectFmt(1)
    format (rtTotal,rangeTable,rtUser)
  )

  val updateRangeDeclaredSt = conn prepareStatement (
    updateFmt(1)
    format (rangeTable,rtDeclared,rtUser)
    )

  val selectRangeDeclaredSt = conn prepareStatement (
    selectFmt(1)
    format (rtDeclared,rangeTable,rtUser)
  )

  val updateRangeFlagsSt = conn prepareStatement (
    updateFmt(1)
    format (rangeTable,rtFlags,rtUser)
  )

  val selectRangeFlagsSt = conn prepareStatement (
    selectFmt(1)
    format (rtFlags,rangeTable,rtUser)
  )

  val selectCountUserSt = conn prepareStatement (
    selectCountFmt
    format (rangeTable,rtUser)
  )

  // unchecked: Some(e: T) will not be checked due to type erasure
  def deStream[T] = { x: Any =>
    x match { case Stream(e: T) => Some(e: T); case _ => None }
  }

  // this cannot find x:
  // def deStream[T] = { case Stream(x: T) => x; case _ => None }
  // perhaps we should make deStream into an implicit for the selects below,
  // even back in RichSQL?  Ask @n8han! :)

  case class UserPG(uid: UserID) extends UserDB(uid: UserID) {


    def uRange_=(r: UserTwitRange): Unit =
      insertRangeFullSt << uid <<
        r.range.first << r.range.last << r.range.total <<
        r.declared << r.flags <<!

    /* -- perhaps this original is simpler to follow, and not much longer either:
    def range: Option[UserTwitRange] = {
      val vs = selectRangeFullSt << uid <<! { rs => UserTwitRange(TwitRange(rs,rs,rs),rs,rs) }
      vs match { case Stream(x) => Some(x); case _ => None }
    }
    */

    def uRange: Option[UserTwitRange] = {
      // println(selectRangeFullSt)
      // NB would love to jigger precedences to get rid of enclosing ()
      (selectRangeFullSt << uid <<! { rs => UserTwitRange(TwitRange(rs,rs,rs),rs,rs) }) ->:
      deStream[UserTwitRange]
    }

    def range: Option[TwitRange] =
      (selectRangeSt << uid <<! { rs => TwitRange(rs,rs,rs)}) ->:
      deStream[TwitRange]

    // adjust range
    def rangeFirst_=(ar: AdjustRange): Unit =
      updateRangeFirstSt << ar.endpoint << ar.total << uid <<!

    def rangeLast_=(ar:  AdjustRange): Unit =
      updateRangeLastSt << ar.endpoint << ar.total << uid <<!

    def rangeFirst: Option[TwitID] =
      (selectRangeFirstSt << uid <<! { _.nextLong }) ->:
      deStream[TwitID]

    def rangeLast:  Option[TwitID] =
      (selectRangeFirstSt << uid <<! { _.nextLong }) ->:
      deStream[TwitID]

    def totalTwits: Option[TwitCount] =
      (selectRangeFirstSt << uid <<! { _.nextInt }) ->:
      deStream[TwitCount]

    def declaredTwits_=(d: TwitCount): Unit =
      updateRangeDeclaredSt << d << uid <<!

    def declaredTwits: Option[TwitCount] =
      (selectRangeFirstSt << uid <<! { _.nextInt }) ->:
      deStream[TwitCount]

    def flags_=(i: UserFlags): Unit =
      updateRangeFlagsSt << i << uid <<!

    def flags: Option[UserFlags] =
      (selectRangeFlagsSt << uid <<! { _.nextInt}) ->:
      deStream[UserFlags]
  }

  // can replace hard-coded table with format
  // using ttUser, etc.
  val testRangeSetupSts = Array (
  "drop table if exists " + rangeTable,
  "create table " + rangeTable +
  """
    (uid integer not null,
    first bigint not null,
    last bigint not null,
    total integer not null,
    declared integer not null,
    flags integer not null);
  """)

  def testRange = {
    implicit val s: Statement = conn << testRangeSetupSts

    val u1 = UserPG(1)

    u1.uRange = UserTwitRange(TwitRange(8,23,4),4,0)

    val ur = u1.uRange
    println(ur)
    // assert can go here. u1.uRange.toString == "..."
  }


  val ttTwit = "tid"
  val ttUser = "uid"
  val ttTime = "time"
  val ttText = "text"

  val rrTwit      = "tid"
  val rrReplyTwit = "trep"
  val rrReplyUser = "urep"

  val selectCountTwitSt = conn prepareStatement (
    selectCountFmt
    format (twitTable,ttTwit)
    )

  val insertTwitSt = conn prepareStatement (
    insertFmt(4)
    format (twitTable,ttTwit,ttUser,ttTime,ttText)
    )

  val selectTwitSt = conn prepareStatement (
    selectFmt(3)
    format (ttUser,ttTime,ttText,twitTable,ttTwit)
    )

  val selectCountReplySt = conn prepareStatement (
    selectCountFmt
    format (replyTable,ttTwit)
    )

  val insertReplySt = conn prepareStatement (
    insertFmt(3)
    format (replyTable,rrTwit,rrReplyTwit,rrReplyUser)
    )

  val selectReplySt = conn prepareStatement (
    selectFmt(2)
    format (rrReplyTwit,rrReplyUser,replyTable,rrTwit)
    )

  case class TwitPG(tid: TwitID) extends TwitDB(tid: TwitID) {
    def exists: Boolean = {
      val count: Long = selectCountTwitSt << tid <<! { _.nextLong } match {
        case Stream(x) => x
        case _ => 0
      }
      count match {
        case 0 => false
        case _ => true
      }
    }

    def isReply: Boolean = {
      val count = selectCountReplySt << tid <<! { _.nextLong } match {
        case Stream(x) => x
        case _ => 0
      }
      count match {
        case 0 => false
        case _ => true
      }
    }

    def put(t: Twit): Unit = {
      // NB: if reply, DO TRANSACTION
      insertTwitSt << tid << t.uid << t.time << t.text <<!

      t.reply match {
        case Some(r) =>
          insertReplySt << tid << r.replyTwit << r.replyUser <<!
        case _ => ()
      }
    }

    def getCore: Option[Twit] = {
      (selectTwitSt << tid <<! { rs => Twit(tid,rs,rs,rs,None) }) ->:
      deStream[Twit]
    }

    def getFull: Option[Twit] = {
      val ot = getCore
      ot match {
        case Some(t) => 
          if (isReply) {
            selectReplySt << tid <<! { rs => ReplyTwit(tid,rs,rs) } match {
              case Stream(r) => { Some(Twit(t,r)) }
              case _ => ot
            }
          }
          else ot
        case _ => None
      }
    }
  }

  val testTwitSetupSts = Array (
    "drop table if exists " + twitTable
    , "create table " + twitTable +
    """(tid bigint not null,
    uid integer not null,
    time timestamp not null,
    text varchar(140) not null)"""
    , "drop table if exists " + replyTable
    , "create table " + replyTable +
    """(tid bigint not null,
    trep bigint not null,
    urep integer not null)
    """)

  def testTwit = {
    implicit val s: Statement = conn << testTwitSetupSts

    val t1 = TwitPG(123)

    val now = new DateTime(new java.util.Date)
    t1 put Twit(11,1011,now,"let's all go a-tweetin' and be merry!",
      Some(ReplyTwit(11,9,1007)))

    val t1core = t1.getCore
    println(t1core)
    val t1full = t1.getFull
    println(t1full)
  }
}