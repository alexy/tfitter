package com.tfitter

// import org.talkingpuffin.twitter._
import com.tfitter.db._

object Main {
  // Access would have to be compiled after filtering,
  // http://org.suffix.la/post/124159629/encrypting-and-replacing-passwords-with-maven
  // import Access.{twitterUser, twitterPassword}
  
  // def queryTwitter(twitterUser: String, twitterPassword: String) {
  //   val auth = TwitterSession(twitterUser,twitterPassword)
  //   val tweets = auth.getUserTimeline(twitterUser)
  //   for (t <- tweets) println(t)
  // }
  
  def doPGtest {
    val dbDriver = Class.forName("org.postgresql.Driver")

    val jdbcArgs = {
      import Config.{jdbcUrl,jdbcUser,jdbcPwd,rangeTable,twitTable,replyTable}
      JdbcArgs(jdbcUrl,jdbcUser,jdbcPwd,rangeTable,twitTable,replyTable)
    }

    // val jdbcArgs=JdbcArgs("jdbc:postgresql:twitter","alexyk","","testRange","testTwit","testReply")

    val tdb = new TwitterPG(jdbcArgs)
      
    tdb.testRange
    tdb.testTwit
  }
  

  def showBdb {
    val bdbFlags = BdbFlags(
      false,  // allowCreate
      true,   // readOnly
      false,  // transactional
      false   // deferred write
    )
    val bdbCacheSize = None
    val bdbArgs = {
      import Config.{bdbEnvPath,bdbStoreName}
      BdbArgs(bdbEnvPath,bdbStoreName,bdbFlags,bdbCacheSize)
    }

    val tdb = new TwitterBDB(bdbArgs)

    tdb.showData
  }

  def main(args: Array[String])  {
    println("numCores => "+Config.numCores)

    // doPGtest
    showBdb
  }
}