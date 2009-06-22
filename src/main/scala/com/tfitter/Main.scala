package com.tfitter

// import org.talkingpuffin.twitter._
import com.tfitter.db._
import scala.io._

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
    val tdb = new TwitPG("jdbc:postgresql:twitter","alexyk","","testRange")
      
    tdb.testRange
  }
  
  def propertyTest = {
    // val dbDriver = Class.forName("org.postgresql.Driver")
    // doPGtest
    
    def getProperties = this.getClass getResourceAsStream "/application.properties"
    var st = getProperties
          
    // in order to reset, do ss = ss.reset // !
    var ss = BufferedSource.fromInputStream(st, "UTF-8", 512, { 
      () =>  Source.fromInputStream(getProperties) })
      
    val linesIter = ss.getLines
    val twitterUser=linesIter.next.trim
    // can use some simple steganography here
    val twitterPassword=linesIter.next.trim
    
    println("user=>"+twitterUser+", password=>"+twitterPassword)
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

  def main(args: Array[String]) = {
    println("eww, world!")
  }
}