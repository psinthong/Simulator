package main.scala.Simulator

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.io.Source
import Executor.Executor
import main.scala.Scheduler.TaskScheduler
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

/**
 * Created by Gift_Sinthong on 3/11/15.
 */
object Simulator {
  def main (args: Array[String]) {
    val filename = "sleep.txt"
    var sleepTimes = List[Int]()
    var interval = 50
    var isStopped = false
    var ExecutorIdToSleepTime: HashMap[String,Seq[Int]] = new HashMap[String,Seq[Int]]
    val ExecutorIdToExe: HashMap[String,Executor] = new HashMap[String,Executor]
    ExecutorIdToSleepTime.put("1",Seq.fill(500)(Random.nextInt(200)))
    ExecutorIdToSleepTime.put("2",Seq.fill(600)(Random.nextInt(200)))
    ExecutorIdToSleepTime.put("3",Seq.fill(400)(Random.nextInt(200)))

    var scheduler = new TaskScheduler



//    ---------------Global Queue Case ------------------
    var globalQ: LinkedBlockingQueue[Int] = new LinkedBlockingQueue[Int]()
    val randomG = scala.util.Random
    for(a <- 1 to 1500){
      globalQ.add(randomG.nextInt(200))
    }
    var execNum = 3
//    println(globalQ)
//    ---------------------------------------------------


//    for(line <- Source.fromFile(filename).getLines()) {
//      val s: Int = line.toInt
//      sleepTimes = sleepTimes:+s
//    }
//    for (i <- 0 to sleepTimes.size-1) {
//      val thread = new Thread {
//        override def run {
//          println("Sleep :"+sleepTimes(i))
//          val exe = new Executor((i+1).toString())
//          //TO DO create and initialize ExecutorMetrics
//        }
//      }
//      Thread.sleep(sleepTimes(i)) // slow the loop down a bit
//      thread.start
//    }
    val requestNum = 0
    var currentNumParts = 0
    val tScheduler = new TaskScheduler()
    val now = System.currentTimeMillis()
    var exeGaveParts = 0
    for (i <- 0 to ExecutorIdToSleepTime.size-1) {
//      var exeID = (i+1).toString()
//      var sleeps = ExecutorIdToSleepTime.apply(exeID)
     /*val thread = new Thread {
        override def run {
          var attempNum = 0
          var exeID = (i+1).toString()

          var sleeps = ExecutorIdToSleepTime.apply(exeID)
//          currentNumParts = sleeps.size
          val exe = new Executor(exeID, sleeps)

          ExecutorIdToExe.put(exeID, exe) // put each executor in a map to use for work stealing

//          var isCreated = false
//          if (!isCreated){
//            isCreated = true
//            val t = new Thread() {
//              override def run() {
//                while (!isStopped) {
//                  exe.sendHeartbeat()
//                  Thread.sleep(interval)
//                }
//              }
//            }
//            t.setName("Driver Heartbeater")
//            t.start()
//          }

//          while (!sleeps.isEmpty) {
//
//            exe.sendHeartbeat(sleeps)
//            tScheduler.executorHeartbeatReceived(exeID,exe.executorMetrics.get)
////            println("Executor ID : "+exeID+" Sleep :" + sleeps(0))
////            val seqExecs =tScheduler.getExecutorToStealFrom1
////            for (j <- 0 to seqExecs.size-1) {
////              println("Steal from Exec : " + seqExecs(j))
////            }
//            Thread.sleep(sleeps(0))
//            sleeps = sleeps.drop(1)
////            println("Number of metrics : "+tScheduler.executorIdToExMetrics.size)
//          }

          while (!exe.partitionIdsToProcess.isEmpty) {
            val sleepTime =exe.partitionIdsToProcess.toArray()(0)
            exe.sendHeartbeat(exe.partitionIdsToProcess)
            tScheduler.executorHeartbeatReceived(exeID,exe.executorMetrics.get)

            Thread.sleep(sleepTime.toString.toLong)
            exe.partitionIdsToProcess.poll()
//            sleeps = sleeps.drop(1)
//            println("Partition to process : "+exe.partitionIdsToProcess.size())

//            if ( exe.partitionIdsToProcess.size <= exe.currentNumparts / 3 && attempNum < 3 && exeID != exeGaveParts){
              attempNum = attempNum+1
            if (exe.partitionIdsToProcess.isEmpty){
              val seqExecIDs =tScheduler.getExecutorToStealFrom1 // get Exe to steal from
              if(seqExecIDs(0).toString != exeID){ // not to steal from itself
                attempNum = 0 // reset number of attempt to steal
                exeGaveParts = seqExecIDs(0).toInt // set flag of exe that just gave partitions
                exe.enterStealMode()
                var seqExectoStealFrom =  Seq.empty[Executor]
                //            for (i <- 0 to seqExecIDs.length-1){
                //              seqExectoStealFrom  ++= ExecutorIdToExe.get(seqExecIDs(i).toString())
                //
                //            }
                //            println("Try to steal from : "+seqExectoStealFrom(0).getId())

                seqExectoStealFrom ++= ExecutorIdToExe.get(seqExecIDs(0).toString())
                exe.StealFrom(seqExectoStealFrom)
                exe.currentNumparts = exe.partitionIdsToProcess.size()

              }
              else if(seqExecIDs(0).toString == exeID && seqExecIDs(1).toString != exeID){
                exeGaveParts = seqExecIDs(1).toInt // set flag of exe that just gave partitions
                attempNum = 0 // reset number of attempt to steal
                exe.enterStealMode()
                var seqExectoStealFrom =  Seq.empty[Executor]
                seqExectoStealFrom ++= ExecutorIdToExe.get(seqExecIDs(1).toString())
                exe.StealFrom(seqExectoStealFrom)
                exe.currentNumparts = exe.partitionIdsToProcess.size()
              }

            }

          }
          val total = System.currentTimeMillis()-now
          println("Total time : " + (total.toFloat/1000.toFloat) + "sec.")



          // val exe = new Executor((i+1).toString())
          //TO DO create and initialize ExecutorMetrics
        }
      }*/
//      thread.setDaemon(true)


//-----------------------Uncomment to test Global Q-----------------------
     val thread = new Thread {
       override def run {
         var exeID = (i+1).toString()

         var sleeps = Seq.empty[Int]
         for(n<- 1 to execNum){
           var temp = globalQ.poll()
           sleeps ++= Seq(temp)
         }
//         println("sleep : "+sleeps)
         currentNumParts = sleeps.size
         val exe = new Executor(exeID, sleeps)
         ExecutorIdToExe.put(exeID, exe) // put each executor in a map to use for work stealing



         while (!exe.partitionIdsToProcess.isEmpty) {
           val sleepTime =exe.partitionIdsToProcess.toArray()(0)
           exe.sendHeartbeat(exe.partitionIdsToProcess)
           tScheduler.executorHeartbeatReceived(exeID,exe.executorMetrics.get)

           Thread.sleep(sleepTime.toString.toLong)
           exe.partitionIdsToProcess.poll()
           if ( exe.partitionIdsToProcess.size <= Math.sqrt(currentNumParts) && !globalQ.isEmpty){
//           if ( exe.partitionIdsToProcess.size <= currentNumParts/ 3 && !globalQ.isEmpty){
//           if (exe.partitionIdsToProcess.isEmpty && !globalQ.isEmpty){

             globalQ = exe.StealFrom2(globalQ,execNum)
                           currentNumParts = exe.partitionIdsToProcess.size()

           }

         }
         val total = System.currentTimeMillis()-now
         println("Total time : " + (total.toFloat/1000.toFloat) + "sec.")


       }
     }
      thread.start
    }


  }



}
