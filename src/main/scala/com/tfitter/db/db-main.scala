package com.tfitter

import com.tfitter.Status._
import org.suffix.util.bdb.{BdbArgs,BdbFlags}

import System.err
import java.io.PrintStream

object Db {
  // simple testing version where inserters just print their inputs
  def printer: Array[String] => Unit = { args => 

    val numThreads = Config.numCores

    // make this a parameter:
    val showingProgress = true

    Console.setOut(new PrintStream(Console.out, true, "UTF8"))
    err.println("[this is stderr] Welcome to Twitter Gardenhose JSON Extractor in IDEA")
    // Console.println("this is console")

    val readLines = new ReadLines(args(0),numThreads,showingProgress)

    // before I added type annotation List[Inserter] below,
    // I didn't realize I'm not using a List but get a Range...  added .toList below
    val inserters: List[Printer] = (0 until numThreads).toList map (new Printer(_))

    val parsers: List[JSONExtractor] = inserters map (ins => new JSONExtractor(ins.id,readLines,ins))

    (parsers zip inserters) foreach { case (parser, inserter) =>
      inserter.setExtractor(parser) }
    
    readLines.start
    inserters foreach (_.start)
    parsers foreach (_.start)
  }


  def theBdbArgs(args: BdbInserterArgs): BdbArgs = {
    val bdbEnvPath = args.envName getOrElse Config.bdbEnvPath
    val bdbStoreName = args.storeName getOrElse Config.bdbStoreName
    val bdbCacheSize = args.cacheSize match {
      case Some(x) => Some((x*1024*1024*1024).toLong)
      case _ => Config.bdbCacheSize
    }
    val bdbFlags = BdbFlags(
      args.allowCreate   getOrElse false,
      args.readOnly      getOrElse false,
      args.transactional getOrElse false,
      args.deferredWrite getOrElse false,
      args.noSync        getOrElse false
    )
    val bdbVerbose = args.bdbVerbose getOrElse false
    val bdbArgs = BdbArgs(bdbEnvPath,bdbStoreName,bdbFlags,bdbCacheSize,bdbVerbose)    
    bdbArgs
  }

  def inserterBDB(args: BdbInserterArgs) {
    import org.suffix.util.bdb.{BdbArgs,BdbFlags}

    val numThreads = args.numThreads getOrElse 1 // or Config.numCores
    val bdbArgs = theBdbArgs(args)

    // make this a parameter:
    val showingProgress = true

    Console.setOut(new PrintStream(Console.out, true, "UTF8"))
    err.println("[this is stderr] Welcome to Twitter Gardenhose JSON Extractor in IDEA")
    // Console.println("this is console")

    // don't even need ti import java.sql.DriverManager for this,
    // magic -- NB see excatly what it does:

    val readLines = new ReadLines(args.fileName,numThreads,showingProgress)

    // before I added type annotation List[Inserter] below,
    // I didn't realize I'm not using a List but get a Range...  added .toList below
    val inserters: List[BdbInserter] = (0 until numThreads).toList map (new BdbInserter(_,bdbArgs))

    val parsers: List[JSONExtractor] = inserters map (ins => new JSONExtractor(ins.id,readLines,ins))

    (parsers zip inserters) foreach { case (parser, inserter) =>
      inserter.setExtractor(parser) }

    readLines.start
    inserters foreach (_.start)
    parsers foreach (_.start)
  }
}


case class BdbInserterArgs (
  fileName: String,
  envName: Option[String],
  storeName: Option[String],
  numThreads: Option[Int],
  cacheSize: Option[Double],
  allowCreate: Option[Boolean],
  readOnly: Option[Boolean],
  transactional: Option[Boolean],
  deferredWrite: Option[Boolean],
  noSync: Option[Boolean],
  bdbVerbose: Option[Boolean]
  ) {
    override def toString: String = 
    "fileName:"+fileName+
    "\n  envName:"+envName+
    ", storeName:"+storeName+
    ", numThreads:"+numThreads+
    ", cacheSize:"+cacheSize+
    "\n  allowCreate:"+allowCreate+
    ", readOnly:"+readOnly+
    ", transactional:"+transactional+
    ", deferredWrite:"+deferredWrite+
    ", noSync:"+noSync+
    ", bdbVerbose:"+bdbVerbose
  }


object MainInsertBDB extends optional.Application {
  def main(
    fileName: String,
    envName: Option[String],
    storeName: Option[String],
    numThreads: Option[Int],
    cacheSize: Option[Double],
    allowCreate: Option[Boolean],
    readOnly: Option[Boolean],
    transactional: Option[Boolean],
    deferredWrite: Option[Boolean],
    noSync: Option[Boolean],
    progress: Option[Long],
    justPrint: Option[Boolean],
    bdbVerbose: Option[Boolean],
    actors: Option[Boolean]   
    ) = {
      
    val args = BdbInserterArgs(
      fileName,
      envName,
      storeName,
      numThreads,
      cacheSize,
      allowCreate,
      readOnly,
      transactional,
      deferredWrite,
      noSync,
      bdbVerbose
      )
      
    val progress_ : Option[Long] = Some(progress getOrElse 10000)
    val actors_ = actors getOrElse false
    val justPrint_ = justPrint getOrElse false
    
    if (justPrint_) err.println("Printing Twits to Screen:")
    else err.println("BDB Inserter Args:"+args)
    
    if (actors_) {
      err.println("using actors")
      if (justPrint_) {
        Db.printer(Array(fileName))      
      } else {    
        Db.inserterBDB(args)
      }
    } else { // single thread
      err.println("using actorless loader")
      val bdbArgs = Db.theBdbArgs(args)

      val loader = if (justPrint_) new PrintStatuses else new BdbInsertStatuses(bdbArgs)      
      
      loader.load(fileName,progress_)
      loader.finish
    }
  }
}
