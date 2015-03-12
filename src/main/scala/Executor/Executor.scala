package Executor

/**
 * Created by shivinkapur on 3/11/15.
 */
import java.io.File
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.io.Source

class Executor(
                executorId: String) {

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


//  val filename = "abc.txt"
//  val sleepTimes = List()
//  val ExecutorIdToSleepTime: HashMap[String,Int] = new HashMap[String,Int]
//  for(line <- Source.fromFile(filename).getLines()) {
//    println(line)
//    val s: Int = line.toInt
//    sleepTimes.++(List(s))
//  }
//
//  for (i <- 0 to sleepTimes.size) {
//    val thread = new Thread {
//
//    }
//    thread.start
//    Thread.sleep(50) // slow the loop down a bit
//  }
  // Maintains the list of running tasks.
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  def launchTask(
                  taskId: Long, taskName: String, serializedTask: ByteBuffer) {

    val tr = new TaskRunner(taskId, taskName, serializedTask)
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
    while(partitionIdsToProcess.isEmpty && isSteal.get()) {
      Thread.sleep(100)
    }
    if(partitionIdsToProcess.isEmpty) {
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
    env.metricsSystem.report()
    isStopped = true
    threadPool.shutdown()
  }


}
