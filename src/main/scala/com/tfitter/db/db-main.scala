package com.tfitter

import com.tfitter.Status._

import System.err
import java.io.PrintStream

object DbMain {
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

  
  def inserterPG(args: Array[String]) {
    import la.scala.sql.rich.RichSQL._
    import com.tfitter.db.JdbcArgs

    val numThreads = Config.numCores
    val jdbcArgs = {
      import Config.{jdbcUrl,jdbcUser,jdbcPwd,rangeTable,twitTable,replyTable}
      JdbcArgs(jdbcUrl,jdbcUser,jdbcPwd,rangeTable,twitTable,replyTable)
    }

    
    // make this a parameter:
    val showingProgress = true
    
    Console.setOut(new PrintStream(Console.out, true, "UTF8"))
    err.println("[this is stderr] Welcome to Twitter Gardenhose JSON Extractor in IDEA")
    // Console.println("this is console")

    // don't even need ti import java.sql.DriverManager for this,
    // magic -- NB see excatly what it does:
    val dbDriver = Class.forName("org.postgresql.Driver")

    val readLines = new ReadLines(args(0),numThreads,showingProgress)
    
    // before I added type annotation List[Inserter] below, 
    // I didn't realize I'm not using a List but get a Range...  added .toList below
    val inserters: List[PGInserter] = (0 until numThreads).toList map (new PGInserter(_,jdbcArgs))

    val parsers: List[JSONExtractor] = inserters map (ins => new JSONExtractor(ins.id,readLines,ins))

    (parsers zip inserters) foreach { case (parser, inserter) =>
      inserter.setExtractor(parser) }

    readLines.start
    inserters foreach (_.start)
    parsers foreach (_.start)
  }


  def inserterBDB(args: Array[String]) {
    import com.tfitter.db.BdbArgs

    val numThreads = 1 // Config.numCores
    // without L suffix. cache size was int and maxed out at 2g!
    // val bdbCacheSize: Option[Long] = Some(3*1024*1024*1024L)
    val bdbArgs = {
      import Config.{bdbEnvPath,bdbStoreName,bdbCacheSize}
      BdbArgs(bdbEnvPath,bdbStoreName,bdbCacheSize)
    }


    // make this a parameter:
    val showingProgress = true

    Console.setOut(new PrintStream(Console.out, true, "UTF8"))
    err.println("[this is stderr] Welcome to Twitter Gardenhose JSON Extractor in IDEA")
    // Console.println("this is console")

    // don't even need ti import java.sql.DriverManager for this,
    // magic -- NB see excatly what it does:

    val readLines = new ReadLines(args(0),numThreads,showingProgress)

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

  def main(args: Array[String]) =
    inserterBDB(args)
    // printer(args)
}