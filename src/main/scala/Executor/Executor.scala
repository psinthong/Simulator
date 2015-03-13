package Executor

/**
 * Created by shivinkapur on 3/11/15.
 */

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean


import main.scala.Executor.ExecutorMetrics
import org.apache.spark.Heartbeat
import org.apache.spark.HeartbeatResponse
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.AkkaUtils

import scala.collection.mutable
import main.scala.Executor.TaskMetrics
import main.scala.Heartbeat

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Source

class Executor(executorId: String) {

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val partitionIdsToProcess: LinkedBlockingQueue[Int] = new LinkedBlockingQueue[Int]()
  private val partitionIdsProcessedSoFar: LinkedBlockingQueue[Int] = new LinkedBlockingQueue[Int]()

  @volatile private var isStopped = false

  private var isSteal: AtomicBoolean = new AtomicBoolean(false) // HERE

  private var numPartitionsProcessedSoFar: Int = 0
  private var numPartitionsProcessedAtPrevHeartBeat: Int = 0

  private var numPartitionsStolenBySoFar: Int = 0
  private var numPartitionsStolenFromSoFar: Int = 0
  private var numHeartbeatsSoFar: Int = 0

  private var skipHeartbeat = false


  // Maintains the list of running tasks.
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  private val interval:Int = 1000
  private val timeout:Int = 1000
  private val retryAttempts:Int = 5
  private val retryIntervalMs:Int = 0
//  private val heartbeatReceiverRef =

  startDriverHeartbeater()


  def launchTask(taskId: Long, taskName: String, serializedTask: ByteBuffer) {

    //val tr = new TaskRunner(taskId, taskName, serializedTask)
    runningTasks.put(taskId, tr)
    // threadPool.execute(tr)

  }

  def killTask(taskId: Long, interruptThread: Boolean) {
    val tr = runningTasks.get(taskId)
    if (tr != null) {
      tr.kill(interruptThread)
    }
  }

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
    sendHeartbeat()
    parts
  }

  def stop() {
    threadPool.shutdown()
  }


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

  def generateMetrics(): ExecutorMetrics = synchronized {
    val em: ExecutorMetrics = new ExecutorMetrics()

    numPartitionsProcessedAtPrevHeartBeat = numPartitionsProcessedSoFar
    numPartitionsProcessedSoFar = partitionIdsProcessedSoFar.size()

    em.remainingMiniPartitions = partitionIdsToProcess.size()
    em.remainingPartitionIds ++= partitionIdsToProcess
    em.processedMiniPartitions = partitionIdsProcessedSoFar.size()
    em.processedPartitionIds ++= partitionIdsToProcess
    em.partitionsStolenBySoFar = numPartitionsStolenBySoFar
    em.partitionsStolenFromSoFar = numPartitionsStolenFromSoFar

    em.avgPartitionsProcessedPerHeartbeat = numPartitionsProcessedSoFar*1.0/numHeartbeatsSoFar
    em.partitionsProcessedSinceLastHeartbeat = numPartitionsProcessedSoFar - numPartitionsProcessedAtPrevHeartBeat

    em
  }

  def startDriverHeartbeater() {
    val t = new Thread() {
      override def run() {
        // Sleep a random interval so the heartbeats don't end up in sync
        Thread.sleep(interval + (math.random * interval).asInstanceOf[Int])

        while (!isStopped) {
          if(skipHeartbeat) {
            skipHeartbeat = false
          } else {
            sendHeartbeat()
          }
          Thread.sleep(interval)
        }
      }
    }
    t.setDaemon(true)
    t.setName("Driver Heartbeater")
    t.start()
  }

  private[spark] def sendHeartbeat() {
    numHeartbeatsSoFar += 1
    val tasksMetrics = new ArrayBuffer[(Long, TaskMetrics)]()
    for (taskRunner <- runningTasks.values()) {
      if (!taskRunner.attemptedTask.isEmpty) {
        Option(taskRunner.task).flatMap(_.metrics).foreach { metrics =>
          metrics.updateShuffleReadMetrics
          metrics.updateMultiIteratorMetrics
          if(metrics.multiIteratorMetrics.isDefined) {
            partitionIdsProcessedSoFar.addAll(metrics.multiIteratorMetrics.get.processedPartitionIds)
          }
          tasksMetrics += ((taskRunner.taskId, metrics))
        }
      }
    }

    val em = generateMetrics()
    tasksMetrics.foreach(r => r._2.setExecutorMetrics(em))

    val message = Heartbeat(executorId, tasksMetrics.toArray, env.blockManager.blockManagerId)
    val response = AkkaUtils.askWithReply[HeartbeatResponse](message, heartbeatReceiverRef,
      retryAttempts, retryIntervalMs, timeout)
    if (response.reregisterBlockManager) {
      logWarning("Told to re-register on heartbeat")
      env.blockManager.reregister()
    }
  }
}

