package com.tfitter

import db.{TwitterBDB,BdbArgs,BdbFlags}
import System.err

object Walk {
  def main(args: Array[String]) {
    // non-transactional read-only
    val bdbFlags = BdbFlags(
      false,  // allowCreate
      true,   // readOnly
      false,  // transactional
      false   // deferred write
    )

    val bdbCacheSize = None
    
    val bdbArgs = {
      import Config.{bdbEnvPath,bdbStoreName,bdbCacheSize}
      BdbArgs(bdbEnvPath,bdbStoreName,bdbFlags,bdbCacheSize)
    }

    err.println("Twitter BDB contains the following users:")
    val tdb = new TwitterBDB(bdbArgs)
    for (us <- tdb.allUserStats) println(us)
  }
}