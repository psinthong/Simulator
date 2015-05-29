package Executor

/**
 * Created by shivinkapur on 3/11/15.
 */

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{LinkedBlockingQueue, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicBoolean


import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}

import main.scala.Executor.ExecutorMetrics
//import org.apache.spark.Heartbeat
//import org.apache.spark.HeartbeatResponse
//import org.apache.spark.executor.TaskMetrics
//import org.apache.spark.util.AkkaUtils

import scala.collection.mutable
import main.scala.Executor.TaskMetrics
import main.scala.Heartbeat

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Source

class Executor(executorId: String, numParts: Seq[Int]) {
  private val exeId = executorId
  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

//  private val partitionIdsToProcess: LinkedBlockingQueue[Int] = new LinkedBlockingQueue[Int]()
  val partitionIdsToProcess: LinkedBlockingQueue[Int] = new LinkedBlockingQueue[Int]()
  private val partitionIdsProcessedSoFar: LinkedBlockingQueue[Int] = new LinkedBlockingQueue[Int]()

  @volatile private var isStopped = false

  private var isSteal: AtomicBoolean = new AtomicBoolean(false) // HERE

  private var numPartitionsProcessedSoFar: Int = 0
  private var numPartitionsProcessedAtPrevHeartBeat: Int = 0

  private var numPartitionsStolenBySoFar: Int = 0
  private var numPartitionsStolenFromSoFar: Int = 0
  private var numHeartbeatsSoFar: Int = 0

  private var skipHeartbeat = false
  private val partitionIdsPendingStealApproval: LinkedBlockingQueue[Int] = new LinkedBlockingQueue[Int]()

  // Maintains the list of running tasks.
//  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  private val interval:Int = 5
  private val timeout:Int = 1000
  private val retryAttempts:Int = 5
  private val retryIntervalMs:Int = 0
  var currentNumparts:Int = numParts.size
//  private val heartbeatReceiverRef =

  startDriverHeartbeater()
  addPartitions(numParts)

  var executorMetrics: Option[ExecutorMetrics] = None

  @transient private lazy val executorMetricsHistory: ArrayBuffer[ExecutorMetrics] =
    new ArrayBuffer[ExecutorMetrics]()

//  def launchTask(taskId: Long, taskName: String, serializedTask: ByteBuffer) {
//
//    //val tr = new TaskRunner(taskId, taskName, serializedTask)
//    runningTasks.put(taskId, tr)
//    // threadPool.execute(tr)
//
//  }

//  def killTask(taskId: Long, interruptThread: Boolean) {
//    val tr = runningTasks.get(taskId)
//    if (tr != null) {
//      tr.kill(interruptThread)
//    }
//  }

  def limbo(): Option[Int] = {
    while (partitionIdsToProcess.isEmpty && isSteal.get()) {
      Thread.sleep(100)
    }
    if (partitionIdsToProcess.isEmpty) {
      None
    } else {
      Some(partitionIdsToProcess.poll())
    }
  }

  def disableSteal = {
    isSteal.set(false)
  }


  def addPartitions(partitions: Seq[Int]) = partitionIdsToProcess.addAll(partitions)

  // Put logic for deciding how many partitions here. Currently one
  def removePartitions() = {
    val parts = Seq(partitionIdsToProcess.poll())
//    sendHeartbeat()
    parts
  }

//  def stop() {
//    threadPool.shutdown()
//  }


  def stealFractionOfRemainingPartitions(frac: Double = 0.5): Seq[Int] = synchronized {
    val ids: ArrayBuffer[Int] = new ArrayBuffer[Int]()
    val numToSteal: Int = Math.floor(frac * partitionIdsToProcess.size()).toInt

    var temp = -1
    while (numToSteal > 0 && !partitionIdsToProcess.isEmpty) {
      temp = partitionIdsToProcess.poll()
      ids += temp
    }
    ids.toSeq
  }

  def generateMetrics(numParts: LinkedBlockingQueue[Int]): ExecutorMetrics = synchronized {
//    partitionIdsToProcess.drop(partitionIdsToProcess.size())
//    partitionIdsToProcess.addAll(numParts)
    val em: ExecutorMetrics = new ExecutorMetrics()

    numPartitionsProcessedAtPrevHeartBeat = numPartitionsProcessedSoFar
    numPartitionsProcessedSoFar = partitionIdsProcessedSoFar.size()

//    em.remainingMiniPartitions = partitionIdsToProcess.size()
    em.remainingMiniPartitions = numParts.size
//    em.remainingPartitionIds ++= partitionIdsToProcess
    em.processedMiniPartitions = partitionIdsProcessedSoFar.size()
//    em.processedPartitionIds ++= partitionIdsToProcess
    em.partitionsStolenBySoFar = numPartitionsStolenBySoFar
    em.partitionsStolenFromSoFar = numPartitionsStolenFromSoFar

    em.avgPartitionsProcessedPerHeartbeat = numPartitionsProcessedSoFar*1.0/numHeartbeatsSoFar
    em.partitionsProcessedSinceLastHeartbeat = numPartitionsProcessedSoFar - numPartitionsProcessedAtPrevHeartBeat
    println("Exe "+exeId+" has num parts = "+em.remainingMiniPartitions)
    em
  }

  private def setExecutorMetrics(em: ExecutorMetrics): Unit = {
    executorMetricsHistory += em
    executorMetrics = Some(em)
  }

  private def updateExecutorMetrics() = synchronized {
    executorMetrics = executorMetricsHistory.headOption
  }

  def startDriverHeartbeater() {
    Thread.sleep(interval+ (math.random * interval).asInstanceOf[Int])
//    sendHeartbeat()
//    val t = new Thread() {
//      override def run() {
//        // Sleep a random interval so the heartbeats don't end up in sync
//        Thread.sleep(interval+ (math.random * interval).asInstanceOf[Int])
//        while (!isStopped) {
//          if(skipHeartbeat) {
//            skipHeartbeat = false
//          } else {
//            sendHeartbeat()
//          }
//          Thread.sleep(interval)
//        }
//      }
//    }
////    t.setDaemon(true)
//    t.setName("Driver Heartbeater")
//    t.start()
  }

  def sendHeartbeat(numParts: LinkedBlockingQueue[Int]) {

//    println("Heartbeat from Executor ID: "+executorId)
    numHeartbeatsSoFar += 1
//    val tasksMetrics = new ArrayBuffer[(Long, TaskMetrics)]()
//    for (taskRunner <- runningTasks.values()) {
//      if (!taskRunner.attemptedTask.isEmpty) {
//        Option(taskRunner.task).flatMap(_.metrics).foreach { metrics =>
//          metrics.updateShuffleReadMetrics
//          metrics.updateMultiIteratorMetrics
//          if(metrics.multiIteratorMetrics.isDefined) {
//            partitionIdsProcessedSoFar.addAll(metrics.multiIteratorMetrics.get.processedPartitionIds)
//          }
//          tasksMetrics += ((taskRunner.taskId, metrics))
//        }
//      }
//    }
//
    val em = generateMetrics(numParts: LinkedBlockingQueue[Int])
    setExecutorMetrics(em)
//    tasksMetrics.foreach(r => r._2.setExecutorMetrics(em))

//    val message = Heartbeat(executorId, tasksMetrics.toArray, env.blockManager.blockManagerId)
//    val response = AkkaUtils.askWithReply[HeartbeatResponse](message, heartbeatReceiverRef,
//      retryAttempts, retryIntervalMs, timeout)
//    if (response.reregisterBlockManager) {
//      logWarning("Told to re-register on heartbeat")
//      env.blockManager.reregister()
//    }
  }

  def enterStealMode() {
    isSteal.set(true)
    println("Executor : "+ executorId +" is entering stealing mode")
//    var result = partitionIdsToProcess.synchronized {
//      if (partitionIdsToProcess.isEmpty) {
//        None
//      } else {
//        Some(partitionIdsToProcess.poll())
//      }
//    }
//    if(!result.isDefined) {
//      isSteal.get match {
//        case 0 => isSteal.set(1)
//          println(s"Executor $executorId is entering stealing mode")
//        case _ =>
//      }
//      result = limbo()
//    }
//    result
  }

  def StealFrom(execs: Seq[Executor]) {
//    this.selectExecsToStealFrom(execs)
    val partsTogive = requestPartitions(execs.head,Int.MaxValue)
    if(partsTogive.size != 0){
      this.partitionIdsToProcess.addAll(partsTogive)
    }


  }

  def StealFrom2(globalQ: LinkedBlockingQueue[Int], execNum: Int): LinkedBlockingQueue[Int] = {
    //    this.selectExecsToStealFrom(execs)
//    val partsTogive = requestPartitions(execs.head, Int.MaxValue)
    var partsToOffer: List[Int] = List()
    if (globalQ.size != 0) {

//      while(partsToOffer.size < 1){
      while(globalQ.size >= partsToOffer.size * 3){
//
//        partsToOffer = globalQ.takeRight(1).toString().toInt :: partsToOffer
        partsToOffer = globalQ.poll() :: partsToOffer
//        partitionIdsToProcess.add(globalQ.poll())
      }
      partitionIdsToProcess.addAll(partsToOffer)
//      this.partitionIdsToProcess.addAll(partsTogive)
    }
    println("Exe "+exeId+" is asking for " +partsToOffer.size+" parts")
    println("Global Q size : " + globalQ.size())
    globalQ
  }

  def selectExecsToStealFrom(execs: Seq[Executor]): Unit = {
    if (execs.nonEmpty) {
      println("Stealing partition from executors " + execs.toString())
      tryStealingFromExec(execs.head)
    } else {
     //closeTasks()
    }
  }

  def tryStealingFromExec(exec: Executor): Unit = {
    val maxWillingToHelp: Int = Int.MaxValue
     this.tryStealing(exec, maxWillingToHelp)
  }
  def tryStealing(exec: Executor, max: Int) = {
//    context.actorSelection(exec.path) ! RequestPartitions(max)
      exec.requestPartitions(exec, max)
      //exec.offerPartitionsToSteal(max)
  }

  def requestPartitions(exec: Executor,max: Int): Seq[Int] = {
    val parts = exec.offerPartitionsToSteal(max)
    if(!parts.isEmpty) {
      println("Asking approval for uploading " + parts.size + " partitions")


//      driver ! SeekStealApproval(executorId, parts, sender)
    } else {
//      logInfo("No partitions to upload to " + sender.toString())
//      sender ! ReceiveStolenPartitions(Seq())
    }
    parts
  }


  def offerPartitionsToSteal(max: Int): Seq[Int] = {
    var partsToOffer: List[Int] = List()
    println("Now exe "+exeId+" has "+partitionIdsToProcess.size()+ " parts")
    while (partsToOffer.size < max && partitionIdsToProcess.size >= partsToOffer.size * 3) {
//    while (partsToOffer.size < max && partitionIdsToProcess.size >= math.pow(partsToOffer.size , 2)) {
      //Use some logic to add partitions to this sequence
      //remove them from the working queue and place them in
      //the pendingStealApproval queue

      partsToOffer = partitionIdsToProcess.poll() :: partsToOffer

    }
    println("Now exe "+exeId+" has "+partitionIdsToProcess.size()+ " parts")
    if (partsToOffer.nonEmpty) {
      partitionIdsPendingStealApproval.addAll(partsToOffer)
    }

    if(partitionIdsToProcess.size() == 0){
      val a = Seq.empty[Int]
      partsToOffer = a.toList
    }
    partsToOffer
  }

  def receiveStolenPartitions(parts: Seq[Int]): Unit = {
    if (!parts.isEmpty) {
      partitionIdsToProcess.addAll(parts)
      numPartitionsStolenBySoFar += parts.length
//      backend.get.confirmReceivedPartition(parts)
//      sendHeartbeat()
    }
//    closeTasks()
  }
  def getId(): String={
    exeId
  }

}

