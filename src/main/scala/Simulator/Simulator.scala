package main.scala.Simulator

import scala.collection.mutable.HashMap
import scala.io.Source
import Executor.Executor
/**
 * Created by Gift_Sinthong on 3/11/15.
 */
class Simulator {
  val filename = "abc.txt"
  val sleepTimes = List()
  val ExecutorIdToSleepTime: HashMap[String,Int] = new HashMap[String,Int]
  for(line <- Source.fromFile(filename).getLines()) {
    println(line)
    val s: Int = line.toInt
    sleepTimes.++(List(s))
  }

  for (i <- 0 to sleepTimes.size) {
    val thread = new Thread {
      override def run {
        Executor e = new Executor((i+1).toString())
      }
    }

        Thread.sleep(sleepTimes(i)) // slow the loop down a bit
        thread.start
  }
}
