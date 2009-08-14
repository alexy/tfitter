package com.tfitter.corpus

import aliasi.tokenizer._
import java.util.regex.Pattern
import com.aliasi.util.ScoredObject
import System.err
import com.aliasi.corpus.{TextHandler,Corpus}
import com.aliasi.lm.TokenizedLM
import db.{TwitterDB, TwitterBDB, TwIterator}

// java.util.SortedSet.toList
import scala.collection.jcl.Conversions._

case class NGramCount (
  nGram: Int,
  count: Int
)

case class ShowTopNGrams (
  often: Long,
  nGramCount: NGramCount
)

import org.suffix.util.bdb.{BdbFlags, BdbArgs}
class TwitterCorpus(bdbArgs: BdbArgs) extends Corpus[TextHandler] {
  override def visitTest(handler: TextHandler): Unit = {}
  // TODO we need to report progress from LM with top freq. terms
  // every so often, which needs treating TextHandler as TokenizedLM,
  // so we define a new visitLm below
  override def visitTrain(handler: TextHandler): Unit = {}

  // Iterator.take(Int) -- not Long, unacceptable generally!
  private var maxTwits: Option[Int] = None
  def setMaxTwits(m: Int): Unit = maxTwits = Some(m)
  def unsetMaxTwits: Unit = maxTwits = None

  private var twitsProgress: Option[Int] = None
  def setTwitsProgress(m: Int): Unit = twitsProgress = Some(m)
  def unsetTwitsProgress: Unit = twitsProgress = None

  var showNGrams: Option[ShowTopNGrams] = None

  def visitLm(lm: TokenizedLM): Unit = {
    val twits: TwIterator = tdb.allTwits
    val twitsToGo = maxTwits match {
      case Some(m) => twits.take(m)
      case _ => twits
    }
    if (twitsToGo.hasNext) err.println("got twits")
    // get the total here as it's "remaining"!
    var twitCount: Long = 0
    var dumpCount: Int  = 0
    for (t <- twitsToGo) {
      // err.println(totalTwits+": "+t.text)
      // TODO lm.train is same as lm.handle?
      lm.train(t.text.toCharArray,0,t.text.length)
      twitCount += 1
      if (!twitsProgress.isEmpty && twitCount % twitsProgress.get == 0) err.print(".")
      showNGrams match {
        case Some(x) if (twitCount % x.often == 0) =>
          dumpCount += 1
          err.println("#"+dumpCount+" intermediate dump of "+LM.showNGramCount(x.nGramCount))
          LM.showTopNGrams(lm,x.nGramCount)
        case _ =>
      }
    }
    err.println("did "+twitCount+" twits.")
  }

  val tdb: TwitterDB = new TwitterBDB(bdbArgs)
}

object Pipe extends optional.Application {

  def main(
    envName: Option[String],
    storeName: Option[String],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],
    maxTwits: Option[Int],
    showProgress: Option[Int],
    gram: Option[Int],
    topGram: Option[Int],
    top: Option[Int],
    showTops: Option[Long],
    args: Array[String]) = {

    val gram_ = gram getOrElse 2
    val topGram_ = topGram getOrElse 2
    val top_ = top getOrElse 20
    val nGramCount = NGramCount(topGram_,top_)

    val bdbEnvPath   = envName getOrElse "bdb"
    val bdbStoreName = storeName getOrElse "twitter"

    val bdbCacheSize = cacheSize match {
      case Some(x) => Some((x*1024*1024*1024).toLong)
      case _ => None // Config.bdbCacheSize
    }
    val bdbFlags = BdbFlags(
      allowCreate   getOrElse false,
      readOnly      getOrElse true,
      transactional getOrElse false,
      deferredWrite getOrElse false,
      noSync        getOrElse false
    )
    val bdbArgs = BdbArgs(bdbEnvPath,bdbStoreName,bdbFlags,bdbCacheSize)

    val twitCorpus = new TwitterCorpus(bdbArgs)
    if (!showProgress.isEmpty) twitCorpus.setTwitsProgress(showProgress.get)
    twitCorpus.setMaxTwits(maxTwits getOrElse 100)
    twitCorpus.showNGrams = showTops match {
      case Some(often) => Some(ShowTopNGrams(often,NGramCount(topGram_,top_)))
      case _ => None
    }
    val tf: TokenizerFactory = LM.twitTokenizerFactory
    val lm = new TokenizedLM(tf,gram_)

    twitCorpus.visitLm(lm)

    err.println("the final dump of "+LM.showNGramCount(nGramCount))    
    LM.showTopNGrams(lm,nGramCount)
  }

}

object LM {
  def showTopNGrams(lm: TokenizedLM, nGramCount: NGramCount) = {
    val NGramCount(nGram,count) = nGramCount
    val freqTerms: List[ScoredObject[Array[String]]] = lm.frequentTermSet(nGram, count).toList

    for (so <- freqTerms) {
      println(so.score+": "+so.getObject.toList.mkString(","))
    }
  }

  def twitTokenizerFactory: TokenizerFactory = {
      var factory = IndoEuropeanTokenizerFactory.INSTANCE
      factory = new RegExFilteredTokenizerFactory(factory,Pattern.compile("\\p{Alpha}+"))
      factory = new LowerCaseTokenizerFactory(factory)
      factory = new EnglishStopTokenizerFactory(factory)
      factory
  }

  def showNGramCount(nc: NGramCount) = {
    nc.count+" top "+nc.nGram+"-grams"
  }
}