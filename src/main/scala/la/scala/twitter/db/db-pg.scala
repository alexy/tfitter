package la.scala.twitter.db

import java.sql.{DriverManager, Connection, ResultSet, PreparedStatement, Statement, Date}
import DriverManager.{getConnection => connect}

import la.scala.util.FunctionNotation._

class TwitPG(jdbcURL: String, user: String, pwd: String, rangeTable: String) extends TwitDB {

    val rtUser     = "uid"
    val rtFirst    = "first"
    val rtLast     = "last"
    val rtTotal    = "total"
    val rtDeclared = "declared"
    val rtFlags    = "flags"

    import la.scala.sql.rich.RichSQL._
    
    implicit val conn = connect(jdbcURL, user, pwd)
    
    def commaSeparated(s: String, n: Int): String = List.fill(n)(s) mkString ","
    
    def insertFmt(n: Int): String  = "insert into %s ("+commaSeparated(
      "%s",n)+") values ("+commaSeparated("?",n)+")"
      
    def updateFmt(n: Int): String = {
      val seqwhat = "%s=?"
      "update %s set "+commaSeparated(
      seqwhat,n)+" where "+seqwhat
     }
     
    def selectFmt(n: Int): String =
      "select "+commaSeparated("%s",n)+" from %s where %s = ?"

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
    
    // unchecked: Some(e: T) will not be checked due to type erasure
    def deStream[T] = { x: Any => 
      x match { case Stream(e: T) => Some(e: T); case _ => None }
    }
    
    // this cannot find x:
    // def deStream[T] = { case Stream(x: T) => x; case _ => None }
    // perhaps we should make deStream into an implicit for the selects below,
    // even back in RichSQL?  Ask @n8han! :)
    
    case class UserPG(uid: UserID) extends User(uid: UserID) {

      def uRange_=(r: UTRange): Unit =
        insertRangeFullSt << uid << 
          r.range.first << r.range.last << r.range.total <<
          r.declared << r.flags <<!
      
      /* -- perhaps this original is simpler to follow, and not much longer either:
      def range: Option[UTRange] = {
        val vs = selectRangeFullSt << uid <<! { rs => UTRange(TwitRange(rs,rs,rs),rs,rs) }
        vs match { case Stream(x) => Some(x); case _ => None }
      }
      */
       
      def uRange: Option[UTRange] = {
        println(selectRangeFullSt)
        // NBS would love to jigger precedences to get rid of enclosing ()
        (selectRangeFullSt << uid <<! { rs => UTRange(TwitRange(rs,rs,rs),rs,rs) }) ->:     
        deStream[UTRange]        
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
      
      u1.uRange = UTRange(TwitRange(8,23,4),4,0)
      
      val ur = u1.uRange
      println(ur)
      // assert can go here. u1.uRange.toString == "..."
    }
}