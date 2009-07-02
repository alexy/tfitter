package com.tfitter

// import org.talkingpuffin.twitter._
import com.tfitter.db._

object Main {
  // Access would have to be compiled after filtering,
  // http://la.scala.la/post/124159629/encrypting-and-replacing-passwords-with-maven
  // import Access.{twitterUser, twitterPassword}
  
  // def queryTwitter(twitterUser: String, twitterPassword: String) {
  //   val auth = TwitterSession(twitterUser,twitterPassword)
  //   val tweets = auth.getUserTimeline(twitterUser)
  //   for (t <- tweets) println(t)
  // }
  
  def doPGtest {
    // val dbDriver = Class.forName("org.postgresql.Driver")
    val tdb = new TwitPG("jdbc:postgresql:twitter","alexyk","","testRange","testTwit","testReply")
      
    tdb.testRange
  }
  
  /*
  def testBdb = {
    import la.scala.util.bdb.Env
    
    val env = new Env(new File(".")) with Transactional with AllowCreate
    env { e =>
        println(e)
    }
  }
  */

  def main(args: Array[String])  {
    println("numCores => "+Config.numCores)
  }
}