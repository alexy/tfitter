package com.tfitter.db

import System.err
import org.joda.time.DateTime

class TwitterPG(jdbcURL: String, user: String, pwd: String,
             rangeTable: String, twitTable: String, replyTable: String) extends TwitterDB {
  import types._

  import java.sql.{DriverManager, Connection, ResultSet, PreparedStatement, Statement, Date}
  import DriverManager.{getConnection => connect}

  // report this to IDEA folks -- I do import ->: so this is not unused:
  import org.suffix.util.FunctionNotation._

  val rtUser       = "uid"
  val rtFirst      = "first_twit"
  val rtLast       = "last_twit"
  val rtFirstTime  = "first_time"
  val rtLastTime   = "last_time"
  val rtTotal      = "total_twits"
  val rtDeclared   = "declared_twits"
  val rtFriends    = "num_friends"
  val rtReplyTwits = "reply_twits"
  val rtReplyUsers = "reply_users"
  val rtFlags      = "flags"

  import la.scala.sql.rich.RichSQL._

  implicit val conn = connect(jdbcURL, user, pwd)
  conn.setAutoCommit(false)

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
    insertFmt(11)
    format (rangeTable,rtUser,rtFirst,rtLast,
            rtFirstTime,rtLastTime,rtTotal,rtDeclared,
            rtFriends,rtReplyTwits,rtReplyUsers,rtFlags)
    )

  val selectRangeFullSt = conn prepareStatement (
    selectFmt(10)
    format (rtFirst,rtLast,rtFirstTime,rtLastTime,rtTotal,
            rtDeclared,rtFriends,rtReplyTwits,rtReplyUsers,
            rtFlags,rangeTable,rtUser)
    )

  val updateRangeFirstSt = conn prepareStatement (
    updateFmt(7)
    format (rangeTable,rtFirst,rtFirstTime,rtTotal,rtDeclared,
            rtFriends,rtReplyTwits,rtReplyUsers,rtUser)
    )

  val updateRangeLastSt = conn prepareStatement (
    updateFmt(7)
    format (rangeTable,rtLast,rtLastTime,rtTotal,rtDeclared,
            rtFriends,rtReplyTwits,rtReplyUsers,rtUser)
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

  // this cannot find x:
  // def deStream[T] = { case Stream(x: T) => x; case _ => None }
  // perhaps we should make deStream into an implicit for the selects below,
  // even back in RichSQL?  Ask @n8han! :)

  case class UserPG(uid: UserID) extends UserDB(uid: UserID) {
    def exists: Boolean = {
      // NB UserID hardcoded as nextInt, update to nextLong if desired
      val count: Long = selectCountUserSt << uid <<! { _.nextInt } match {
        case Stream(x) => x
        case _ => 0
      }
      count match {
        case 0 => false
        case _ => true
      }
    }


    def setStats(us: UserStats): Unit =
      insertRangeFullSt << uid << us.firstTwit << us.lastTwit <<
      us.firstTwitTime << us.lastTwitTime << us.totalTwits <<
      us.totalTwitsDeclared << us.numFriends <<
      us.numReplyTwits << us.numReplyUsers << us.flags <<!

    def set: Unit = stats match {
      case Some(x) => setStats(x)
      case _ => ()
    }

    def getStats: Option[UserStats] = {
      (selectRangeFullSt << uid <<! { r => UserStats(uid,r,r,r,r,r,r,r,r,r,r) }) ->:
      deStream[UserStats]
    }

    def get: Unit = {
      val all: Option[UserStats] = getStats
      stats = all
    }

    // adjust range
    def setRangeFirst: Unit = stats match {
      case Some(x) => { import x._
        updateRangeFirstSt << firstTwit << firstTwitTime <<
                totalTwits << totalTwitsDeclared << numFriends <<
                numReplyTwits << numReplyUsers << x.uid <<! }
      case _ => ()
    }

    def setRangeLast: Unit = stats match {
      case Some(x) => { import x._
        updateRangeFirstSt << lastTwit << lastTwitTime <<
                totalTwits << totalTwitsDeclared << numFriends <<
                numReplyTwits << numReplyUsers << x.uid <<! }
      case _ => ()
    }

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

    def setFlags(i: UserFlags): Unit =
      updateRangeFlagsSt << i << uid <<!

    def getFlags: Option[UserFlags] =
      (selectRangeFlagsSt << uid <<! { _.nextInt}) ->:
      deStream[UserFlags]

    
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
            err.println("user "+x.uid+" going backward at twit "+tid)
            firstTwit = tid
            firstTwitTime = t.time
            setRangeFirst
          } else if (tid > lastTwit) {
            lastTwit = tid
            lastTwitTime = t.time
            setRangeLast
          }
        }
        case _ => { stats =
            Some(UserStats(
              uid, tid, tid, t.time, t.time, 1,
              u.statusesCount, u.friendsCount,
              numReplyTwitsAdd, numReplyUsersAdd, 0
            ))
            set
        }
      }
    }
  }

  // can replace hard-coded table with format
  // using ttUser, etc.
  val testRangeSetupSts = Array (
  "drop table if exists " + rangeTable,
  "create table " + rangeTable +
  """(
    %s integer not null,
    %s bigint not null,
    %s bigint not null,
    %s timestamp not null,
    %s timestamp not null,
    %s integer not null,
    %s integer not null,
    %s integer not null,
    %s integer not null,
    %s integer not null,
    %s integer not null);""" format (
       rtUser,
       rtFirst,
       rtLast,
       rtFirstTime,
       rtLastTime,
       rtTotal,
       rtDeclared,
       rtFriends,
       rtReplyTwits,
       rtReplyUsers,
       rtFlags)
    )

  def testRange = {
    implicit val s: Statement = conn << testRangeSetupSts

    val u1 = UserPG(1)

    val now = new DateTime(new java.util.Date)
    u1.setStats(UserStats(u1.uid,88,111,now,now,25,27,33,5,15,0))

    val stats: Option[UserStats] = u1.getStats
    println(stats)
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
      // getting ResultSet closed error once in a while...
      try {
        val count: Long = selectCountTwitSt << tid <<! { _.nextLong } match {
          case Stream(x) => x
          case _ => 0
        }
        count match {
          case 0 => false
          case _ => true
        }
      } catch {
        case _ => false
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
      try {
        insertTwitSt << tid << t.uid << t.time << t.text <<!
      } catch {
          case e: org.postgresql.util.PSQLException =>
            err.println("TOO LONG: "+e+" ["+t.text+"]")
            throw DBError("CANNOT PUT TWIT "+tid)
          case e => err.println("TWIT:"+e)
            throw DBError("CANNOT PUT TWIT "+tid)
      }
      try {
        t.reply match {
          case Some(r) =>
            try {
              insertReplySt << tid << r.replyTwit /* Some(2500000L) */ << r.replyUser <<!
            } catch {
              case e: ClassCastException =>
                  err.println("CAST:"+e+" [ tid="+tid+" rtid="+r.replyTwit+" ruid="+r.replyUser+" ]")
                  err.print("casting meat to Long: ")
                  val rtLongOpt = r.replyTwit match {
                    case Some(tid) =>
                      val res = Some(tid.toLong)
                      err.println(res)
                      res
                    case _ => None
                  }
                  err.println("trying "+rtLongOpt)
                  insertReplySt << tid << rtLongOpt << r.replyUser.toLong <<!
            }
          case _ => ()
        }
      } catch {
        case e: ClassCastException => err.println("STILL CAST:"+e)
           error("CANNOT PUT TWIT "+tid)
        case e => err.println("REPLY:"+e)
          throw DBError("CANNOT PUT TWIT "+tid)
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

  // make the create statement a format for column names
  // may add foreign key constraint on tid in ReplyTwit
  // but that won't hold for gardenhose as the reply-to
  // is not guaranteed to be there!
  val testTwitSetupSts = Array (
    "drop table if exists " + twitTable
    , "create table " + twitTable +
    """(
    %s bigint not null,
    %s integer not null,
    %s timestamp not null,
    %s varchar(140) not null)""" format (
            ttTwit,
            ttUser,
            ttTime,
            ttText
            )
    , "drop table if exists " + replyTable
    , "create table " + replyTable +
    """(
    %s bigint not null,
    %s bigint, -- can often be null
    %s integer not null)""" format (
            rrTwit,
            rrReplyTwit,
            rrReplyUser
            )
    )

  def testTwit = {
    implicit val s: Statement = conn << testTwitSetupSts

    val t1 = TwitPG(123)

    val now = new DateTime(new java.util.Date)
    t1 put Twit(11,1011,now,"let's all go a-tweetin' and be merry!",
      Some(ReplyTwit(11,Some(9),1007)))

    val t2 = TwitPG(248)

    t2 put Twit(105,1011,now,"empty replies are not so empty as they might seem",
      Some(ReplyTwit(105,None,1011)))

    val t1core = t1.getCore
    println(t1core)
    val t1full = t1.getFull
    println(t1full)

    val t2full = t2.getFull
    println(t2full)
  }


  def insertUserTwit(ut: UserTwit): Unit = {
    val UserTwit(user,twit) = ut
    val uid = user.uid
    val tid = twit.tid
    try {

      val t = TwitPG(tid)

      if (t.exists) {
        err.println("ALREADY HAVE TWIT "+tid)
      } else {
        // conn.begin
        val u = UserPG(uid)
        // may declare that as (u,t)
        // as it's already matched:
        u.updateUserForTwit(ut)
        t put twit
        conn.commit
      }
    } catch {
      case e => {
        // err.println(e)
        err.println("ROLLBACK uid="+uid+" tid="+tid)
        conn.rollback
      }
    }
  }
}