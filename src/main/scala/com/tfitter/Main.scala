package com.tfitter

// import org.talkingpuffin.twitter._
import com.tfitter.db._
import la.scala.util.Properties
import System.err

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
    val tdb = new TwitPG("jdbc:postgresql:twitter","alexyk","","testRange")
      
    tdb.testRange
  }
  
  def propertyTest = {
    val numCoresDefault = 1 
    def doDefault(msg: String, name: String, i: Int) = { err.println(msg+" "+name+": "+i); i }

    val numCoresTag = "number.cores"
    val numCores = try { Properties.get(numCoresTag).toInt } catch { 
      case Properties.NotFound(name) => doDefault("using default",name,numCoresDefault)
      case _: NumberFormatException => doDefault("ill-formed number -- using default",numCoresTag,numCoresDefault)
    }
    println("number of cores => " + numCores)
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
    propertyTest
  }
}