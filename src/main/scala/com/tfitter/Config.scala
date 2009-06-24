package com.tfitter

import la.scala.util.Properties
import System.err

object Config {
    
  def getNumCores = {
    val numCoresName = "number.cores"
    err.print("number of cores ")
    val numCores = try { Properties.getInt(numCoresName) } catch { 
      case Properties.NotFound(reason) => err.print("(default) ")
      1
    }
    err.println("= "+numCores)
    numCores
  }
  
  val numCores = getNumCores
}