package com.tfitter.db

object DbPg {
  def inserterPG(args: Array[String]) {
    import org.suffix.sql.rich.RichSQL._
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
}

object MainInsertPG {
  def main(args: Array[String]) =
    Db.inserterPG(args)
}