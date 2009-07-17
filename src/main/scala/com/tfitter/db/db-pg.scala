package com.tfitter.db

import System.err
import org.joda.time.DateTime

case class JdbcArgs(
        url: String,
        user: String,
        pwd: String,
        rangeTable: String,
        twitTable: String,
        replyTable: String)
  
class TwitterPG(jdbcArgs: JdbcArgs) extends TwitterDB {
  
  val JdbcArgs(jdbcUrl,jdbcUser,jdbcPwd,rangeTable,twitTable,replyTable) = jdbcArgs
  import types._

  import java.sql.{DriverManager, Connection, ResultSet, PreparedStatement, Statement, Date}
  import DriverManager.{getConnection => connect}

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

  // could use connection properties to set user, pwd, ssl, etc.
  implicit val conn = connect(jdbcUrl, jdbcUser, jdbcPwd)
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
    
  def selectAllFmt(n: Int): String =
    "select "+commaSeparated("%s",n)+" from %s"

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
    
  val selectRangeAllSt = conn prepareStatement (
    selectAllFmt(11)
    format (rtUser,rtFirst,rtLast,rtFirstTime,rtLastTime,rtTotal,
            rtDeclared,rtFriends,rtReplyTwits,rtReplyUsers,
            rtFlags,rangeTable)
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
    
    def exists: Boolean = !stats.isEmpty

    def storeStats(us: UserStats): Unit = 
      insertRangeFullSt << uid << us.firstTwit << us.lastTwit <<
      us.firstTwitTime << us.lastTwitTime << us.totalTwits <<
      us.totalTwitsDeclared << us.numFriends <<
      us.numReplyTwits << us.numReplyUsers << us.flags <<!
   
    def fetchStats: Option[UserStats] = {
      selectRangeFullSt << uid <<! { r => UserStats(uid,r,r,r,r,r,r,r,r,r,r) } firstOption 
    }


    // adjust range
    def storeRangeFirst: Unit = stats match {
      case Some(x) => { import x._
        updateRangeFirstSt << firstTwit << firstTwitTime <<
                totalTwits << totalTwitsDeclared << numFriends <<
                numReplyTwits << numReplyUsers << x.uid <<! }
      case _ => ()
    }

    def storeRangeLast: Unit = stats match {
      case Some(x) => { import x._
        updateRangeFirstSt << lastTwit << lastTwitTime <<
                totalTwits << totalTwitsDeclared << numFriends <<
                numReplyTwits << numReplyUsers << x.uid <<! }
      case _ => ()
    }

    /*
    def storeFlags(i: UserFlags): Unit =
      updateRangeFlagsSt << i << uid <<!

    def fetchFlags: Option[UserFlags] =
      (selectRangeFlagsSt << uid <<! { _.nextInt}) ->:
      deStream[UserFlags]
    */
  }

  // can replace hard-coded table with format
  // using ttUser, etc.
  val testRangeSetupSts = Array (
  "drop table if exists " + rangeTable,
  "create table " + rangeTable +
  """(
    %s integer not null primary key,
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
    
        
    conn.commit // create table for future use!
  }


  val ttTwit = "tid"
  val ttUser = "uid"
  val ttTime = "time"
  val ttText = "text"
  // may add ttReplyUser and ttReplyTwit right here as well

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

  // TODO should we ORDER BY tid?
  val selectAllTwitsSt = conn prepareStatement (
    selectAllFmt(4)
    format (ttUser,ttTime,ttText,ttTwit,twitTable)
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
    
    def exists: Boolean = !getCore.isEmpty

    def isReply: Boolean =
      getFull match {
        case Some(t) => !t.reply.isEmpty
        case _ => false
      }

    def put(t: Twit): Unit = {
      // NB: if reply, DO TRANSACTION
      try {
        insertTwitSt << tid << t.uid << t.time << t.text <<!
      } catch {
          case e: org.postgresql.util.PSQLException =>
            // if (e.getMessge.startsWith ...) can be a guard, too!
            if (e.getMessage.startsWith("ERROR: invalid byte sequence for encoding \"UTF8\":"))
              throw DBEncoding("CANNOT PUT TWIT PGSQL BAD UTF8 "+e+" tid "+tid)
            else if (e.getMessage.startsWith("ERROR: duplicate"))
              throw DBDuplicate("CANNOT PUT TWIT PGSQL DUPLICATE "+e+" tid "+tid)
            else
              throw DBError("CANNOT PUT TWIT PGSQL: "+e+" tid "+tid)
          case e => err.println("TWIT:"+e)
            throw DBError("CANNOT PUT TWIT "+tid)
      }
      try {
        t.reply match {
          case Some(r) =>
            insertReplySt << tid << r.replyTwit << r.replyUser <<!
          case _ => ()
        }
      } catch {
        case e => err.println("REPLY:"+e)
          throw DBError("CANNOT PUT REPLY TWIT "+tid)
      }
    }

    def getCore: Option[Twit] = {
      selectTwitSt << tid <<! { rs => Twit(tid,rs,rs,rs,None) } firstOption
    }

    def getReply: Option[ReplyTwit] = 
      selectReplySt << tid <<! { rs => ReplyTwit(tid,rs,rs) } firstOption
      
    def getFull: Option[Twit] = {
      val ot = getCore
      ot match {
        case Some(t) =>
          getReply match {
              case Some(r) => { Some(Twit(t,r)) }
              case _ => ot
          }
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
    %s bigint not null primary key,
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
    %s bigint not null primary key,
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

    conn.commit // create tables for future use!
  }
    
    
  // curry make/txn params
  def insertUserTwit(ut: UserTwit): Unit = {    
    val UserTwit(user,twit) = ut
    val uid = user.uid
    val tid = twit.tid
    try {

      val t = TwitPG(tid)

      // no conn.begin in SQL
      t put twit // will cause exception if present and rollback
      val u = UserPG(uid)
      u.updateUserForTwit(ut)
      conn.commit
    } catch {
      case e => {
        err.println(e)
        err.println("ROLLBACK uid="+uid+" tid="+tid)
        conn.rollback
      }
    }
  }
  
  val subParams = SubParams(TwitPG,UserPG,()=>(),conn.commit _,conn.rollback _)
  def insertUserTwitB = insertUserTwitCurry(subParams)(_)
  
  def allUserStatsStream: Stream[UserStats] =
  // for (us <- selectRangeAllSt <<! { r => UserStats(r,r,r,r,r,r,r,r,r,r,r) }) yield us
    selectRangeAllSt <<! { r => UserStats(r,r,r,r,r,r,r,r,r,r,r) }

  // is there a more direct way to get the List instead of Stream via RichSQL?
  def allUserStatsList: List[UserStats] = allUserStatsStream.toList

  class TwIteratorPG extends TwIterator {
    val st: Stream[Twit] = selectAllTwitsSt <<! { r => val tid: TwitID = r
            val reply = { selectReplySt << tid <<! { rs => ReplyTwit(tid,rs,rs) } firstOption }
            Twit(tid,r,r,r,reply) }
    def hasNext = !st.isEmpty
    def next = st.head
    // can get a Stream iterator and implement ours with it,
    // but those next/hasNext wrap the same head/!sEmpty,
    // so it's not really necessary and isn't really shorter...
    // TODO can we structure the class to use .elements shorter?
    //val sit = st.elements // st.iterator in 2.8
    //def hasNext = sit.hasNext
    //def next = sit.next
  }

  def allTwits: TwIterator = new TwIteratorPG
}