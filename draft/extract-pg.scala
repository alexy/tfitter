import db.{JdbcArgs,TwitterPG} // for PostgreSQL backend

  class PGInserter(override val id: Int, jdbcArgs: JdbcArgs) extends Inserter(id) {
    val tdb = new TwitterPG(jdbcArgs)
    def act() = {
      err.println("Inserter "+id+" started, object "+self+",\n"+
              "  talking to parser "+extractor.id + " ["+extractor+"]")
      extractor ! self
      loop {
        react {
          case ut @ UserTwit(_,_) => { /* ut @ UserTwit(user,twit)
             print(user.name+" "+twit.time+": "+twit.text+
              ", tid="+twit.tid+", uid="+user.uid)
             twit.reply match {
               case Some(r) => println(", reply_twit="+r.replyTwit +
               ", reply_user="+r.replyUser)
               case _ => ()
             }
            println
            */
            try {
              // val t = tdb.TwitPG(twit.tid)
              // t put twit
              tdb.insertUserTwit(ut)
            } catch {
              case DBError(msg) => err.println("DB ERROR: "+msg)
            }

            extractor ! self
          }
          case EndOfInput => {
            err.println("Inserter "+id+" exiting.")
            exit()
          }
          case msg => err.println("Inserter "+id+" unhandled message:"+msg)
        }
      }
    }
  }
