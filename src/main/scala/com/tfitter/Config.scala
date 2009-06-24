package com.tfitter

import la.scala.util.Properties
import System.err

object Config {
  val numCores = Properties.getInt("number.cores",1)
}