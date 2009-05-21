package la.scala.twitter

import org.talkingpuffin.twitter._

object Main extends Application {
  val twitterUser = "khrabrov"
  val twitterPassword = readLine

  val auth = TwitterSession(twitterUser,twitterPassword)

  val tweets = auth.getUserTimeline(twitterUser)
  
  for (t <- tweets)
    println(t)
}