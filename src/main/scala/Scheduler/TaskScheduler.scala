package main.scala.Scheduler
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.{Timer, TimerTask}
import main.scala.Executor.ExecutorMetrics
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkEnv
//import org.apache.spark.SparkException
//import org.apache.spark.TaskEndReason
//import org.apache.spark.TaskState
//import org.apache.spark.TaskState._
//import org.apache.spark.executor.ExecutorMetrics
//import org.apache.spark.executor.MultiIteratorMetrics
//import org.apache.spark.executor.TaskMetrics
//import org.apache.spark.scheduler.DAGScheduler
//import org.apache.spark.scheduler.DirectTaskResult
//import org.apache.spark.scheduler.ExecutorLossReason
//import org.apache.spark.scheduler.FIFOSchedulableBuilder
//import org.apache.spark.scheduler.FairSchedulableBuilder
//import org.apache.spark.scheduler.Pool
//import org.apache.spark.scheduler.SchedulableBuilder
//import org.apache.spark.scheduler.SchedulerBackend
//import org.apache.spark.scheduler.SchedulingMode
//import org.apache.spark.scheduler.SchedulingMode._
//import org.apache.spark.scheduler.TaskDescription
//import org.apache.spark.scheduler.TaskResultGetter
//import org.apache.spark.scheduler.TaskSet
//import org.apache.spark.scheduler.TaskSetManager
//import org.apache.spark.scheduler.WorkerOffer
//import org.apache.spark.storage.BlockManagerId
//import org.apache.spark.util.Utils

//import org.apache.spark.TaskState.TaskState
//import org.apache.spark._
//import org.apache.spark.executor.ExecutorMetrics
//import org.apache.spark.executor.MultiIteratorMetrics
//import org.apache.spark.executor.TaskMetrics
//import org.apache.spark.executor.{MultiIteratorMetrics, ExecutorMetrics}
//import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
//import org.apache.spark.storage.BlockManagerId
//import org.apache.spark.util.Utils

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.concurrent.duration._
import scala.language.postfixOps

import scala.util.Random
/**
 * Created by Gift_Sinthong on 3/12/15.
 */
class TaskScheduler {
//  def this(sc: SparkContext) = this(sc, sc.conf.getInt("spark.task.maxFailures", 4))

//  val conf = sc.conf

  // How often to check for speculative tasks
//  val SPECULATION_INTERVAL = conf.getLong("spark.speculation.interval", 100)

  // Threshold above which we warn user initial TaskSet may be starved
//  val STARVATION_TIMEOUT = conf.getLong("spark.starvation.timeout", 15000)

  // CPUs to request per task
//  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
//  val activeTaskSets = new HashMap[String, TaskSetManager]

  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // Which executor IDs we have executors on
  val activeExecutorIds = new HashSet[String]

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  protected val executorsByHost = new HashMap[String, HashSet[String]]

  protected val hostsByRack = new HashMap[String, HashSet[String]]

  protected val executorIdToHost = new HashMap[String, String]

  // Listener object to pass upcalls into
//  var dagScheduler: DAGScheduler = null
//
//  var backend: SchedulerBackend = null
//
//  val mapOutputTracker = SparkEnv.get.mapOutputTracker
//
//  var schedulableBuilder: SchedulableBuilder = null
//  var rootPool: Pool = null
  // default scheduler is FIFO
//  private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")
//  val schedulingMode: SchedulingMode = try {
//    SchedulingMode.withName(schedulingModeConf.toUpperCase)
//  } catch {
//    case e: java.util.NoSuchElementException =>
//      throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
//  }

  // This is a var so that we can reset it for testing purposes.
//  private var taskResultGetter = new TaskResultGetter(sc.env, this)

//  var taskIdToMIMetrics : HashMap[Long, MultiIteratorMetrics] = new HashMap[Long, MultiIteratorMetrics]
  var executorIdToExMetrics: HashMap[String, ExecutorMetrics] = new HashMap[String, ExecutorMetrics]

  def getExecutorToStealFrom: Seq[String] = {

    var maxPartitions: Int = -1
    var execID: String = ""
    var execMap: HashMap[Double, String] = new HashMap[Double, String]
    var execSeq: Seq[String] = Seq()
    //    executorIdToExMetrics.foreach(r => if(r._2.remainingMiniPartitions > maxPartitions) {
    //      execID = r._1
    //      maxPartitions = r._2.remainingMiniPartitions
    //    })
    executorIdToExMetrics.foreach(r => if(r._2.remainingMiniPartitions > 0) {
      execID = r._1
      println("From getExe to steal From!!!!")
      println("exeID : "+execID)
      execMap.put(r._2.avgPartitionsProcessedPerHeartbeat, execID)
    })
    var execMapSorted = (execMap.toSeq.sortWith(_._1 > _._1)).toMap
    execSeq = execMapSorted.values.map(_.toString).toSeq
//    execSeq.take(3)
    execSeq.take(2)
    //(execID,executorIdToExMetrics.get(execID).get.remainingPartitionIds.toSeq)
    //    Seq(execID)
  }

  def getExecutorToStealFrom1: Seq[String] = {

    var maxPartitions: Int = -1
    var execID: String = ""
    var execMap: HashMap[String, Int] = new HashMap[String, Int]
    var execSeq: Seq[String] = Seq()
    //    executorIdToExMetrics.foreach(r => if(r._2.remainingMiniPartitions > maxPartitions) {
    //      execID = r._1
    //      maxPartitions = r._2.remainingMiniPartitions
    //    }


    executorIdToExMetrics.foreach(r => if(r._2.remainingMiniPartitions > 0) {
      execID = r._1
//      println(execID+" has remaining parts : "+r._2.remainingMiniPartitions)
      execMap.put(execID,r._2.remainingMiniPartitions)

    })
//    println("exeMap : "+execMap.size)
//    var execMapSorted = (execMap.toSeq.sortWith(_._1 > _._1)).toMap
    var execMapSorted = (execMap.toSeq.sortWith(_._2 > _._2)).toMap

//    execSeq = execMapSorted.values.map(_.toString).toSeq
    execSeq = execMapSorted.keys.toSeq
//    println("exeMapSorted : "+execSeq.size)
    //    execSeq.take(3)
    execSeq.take(2)
    //(execID,executorIdToExMetrics.get(execID).get.remainingPartitionIds.toSeq)
    //    Seq(execID)
  }



  def newTaskId(): Long = nextTaskId.getAndIncrement()

//  override def start() {
//    backend.start()
//
//    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
//      logInfo("Starting speculative execution thread")
//      import sc.env.actorSystem.dispatcher
//      sc.env.actorSystem.scheduler.schedule(SPECULATION_INTERVAL milliseconds,
//        SPECULATION_INTERVAL milliseconds) {
//        Utils.tryOrExit { checkSpeculatableTasks() }
//      }
//    }
//  }
//
//  override def postStartHook() {
//    waitBackendReady()
//  }
//
//
//
//  /**
//   * Called to indicate that all task attempts (including speculated tasks) associated with the
//   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
//   * cleaned up.
//   */
//  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
//    activeTaskSets -= manager.taskSet.id
//    manager.parent.removeSchedulable(manager)
//    logInfo("Removed TaskSet %s, whose tasks have all completed, from pool %s"
//      .format(manager.taskSet.id, manager.parent.name))
//  }
//
//  /**
//   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
//   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
//   * that tasks are balanced across the cluster.
//   */
//  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
//    SparkEnv.set(sc.env)
//
//    // Mark each slave as alive and remember its hostname
//    // Also track if new executor is added
//    var newExecAvail = false
//    for (o <- offers) {
//      executorIdToHost(o.executorId) = o.host
//      if (!executorsByHost.contains(o.host)) {
//        executorsByHost(o.host) = new HashSet[String]()
//        executorAdded(o.executorId, o.host)
//        newExecAvail = true
//      }
//      for (rack <- getRackForHost(o.host)) {
//        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
//      }
//    }
//
//    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
//    val shuffledOffers = Random.shuffle(offers)
//    // Build a list of tasks to assign to each worker.
//    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
//    val availableCpus = shuffledOffers.map(o => o.cores).toArray
//    val sortedTaskSets = rootPool.getSortedTaskSetQueue
//    for (taskSet <- sortedTaskSets) {
//      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
//        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
//      if (newExecAvail) {
//        taskSet.executorAdded()
//      }
//    }
//
//    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
//    // of locality levels so that it gets a chance to launch local tasks on all of them.
//    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
//    var launchedTask = false
//    for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
//      do {
//        launchedTask = false
//        for (i <- 0 until shuffledOffers.size) {
//          val execId = shuffledOffers(i).executorId
//          val host = shuffledOffers(i).host
//          if (availableCpus(i) >= CPUS_PER_TASK) {
//            for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
//              tasks(i) += task
//              val tid = task.taskId
//              taskIdToTaskSetId(tid) = taskSet.taskSet.id
//              taskIdToExecutorId(tid) = execId
//              activeExecutorIds += execId
//              executorsByHost(host) += execId
//              availableCpus(i) -= CPUS_PER_TASK
//              assert(availableCpus(i) >= 0)
//              launchedTask = true
//            }
//          }
//        }
//      } while (launchedTask)
//    }
//
//    if (tasks.size > 0) {
//      hasLaunchedTask = true
//    }
//    return tasks
//  }
//
//  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
//    var failedExecutor: Option[String] = None
//    synchronized {
//      try {
//        if (state == TaskState.LOST && taskIdToExecutorId.contains(tid)) {
//          // We lost this entire executor, so remember that it's gone
//          val execId = taskIdToExecutorId(tid)
//          if (activeExecutorIds.contains(execId)) {
//            removeExecutor(execId)
//            failedExecutor = Some(execId)
//          }
//        }
//        taskIdToTaskSetId.get(tid) match {
//          case Some(taskSetId) =>
//            if (TaskState.isFinished(state)) {
//              taskIdToTaskSetId.remove(tid)
//              taskIdToExecutorId.remove(tid)
//            }
//            activeTaskSets.get(taskSetId).foreach { taskSet =>
//              if (state == TaskState.FINISHED) {
//                taskSet.removeRunningTask(tid)
//                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
//              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
//                taskSet.removeRunningTask(tid)
//                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
//              }
//            }
//          case None =>
//            logError(
//              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
//                "likely the result of receiving duplicate task finished status updates)")
//                .format(state, tid))
//        }
//      } catch {
//        case e: Exception => logError("Exception in statusUpdate", e)
//      }
//    }
//    // Update the DAGScheduler without holding a lock on this, since that can deadlock
//    if (failedExecutor.isDefined) {
//      dagScheduler.executorLost(failedExecutor.get)
//      backend.reviveOffers()
//    }
//  }
//
//  /**
//   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
//   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
//   * indicating that the block manager should re-register.
//   */
  def executorHeartbeatReceived(
                                          execId: String,
                                          executorMetrics:ExecutorMetrics): Boolean = {

//    if(!executorMetrics.isEmpty]) {
//      println("Metric size : "+executorMetrics)
      executorIdToExMetrics.put(execId, executorMetrics)
//    }

//    val metricsWithStageIds: Array[(Long, Int, Int, TaskMetrics)] = synchronized {
//      taskMetrics.flatMap { case (id, metrics) =>
//        taskIdToTaskSetId.get(id)
//          .flatMap(activeTaskSets.get)
//          .map(taskSetMgr => (id, taskSetMgr.stageId, taskSetMgr.taskSet.attempt, metrics))
//      }
//    }
//    metricsWithStageIds.foreach(r => {
//      r._4.updateMultiIteratorMetrics()
//      if (r._4.multiIteratorMetrics.isDefined) {
//        println("Task " + r._1 + ", metrics: " + r._4.multiIteratorMetrics.get.processedPartitionIds.size + " partitions processed")
//        taskIdToMIMetrics.put(r._1, r._4.multiIteratorMetrics.get)
//      }
//    })
    true
//      executorHeartbeatReceived(execId, executorMetrics)
  }
//
//  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long) {
//    taskSetManager.handleTaskGettingResult(tid)
//  }
//
//  def handleSuccessfulTask(
//                            taskSetManager: TaskSetManager,
//                            tid: Long,
//                            taskResult: DirectTaskResult[_]) = synchronized {
//    taskSetManager.handleSuccessfulTask(tid, taskResult)
//  }
//
//  def handleFailedTask(
//                        taskSetManager: TaskSetManager,
//                        tid: Long,
//                        taskState: TaskState,
//                        reason: TaskEndReason) = synchronized {
//    taskSetManager.handleFailedTask(tid, taskState, reason)
//    if (!taskSetManager.isZombie && taskState != TaskState.KILLED) {
//      // Need to revive offers again now that the task set manager state has been updated to
//      // reflect failed tasks that need to be re-run.
//      backend.reviveOffers()
//    }
//  }
//
//  def error(message: String) {
//    synchronized {
//      if (activeTaskSets.size > 0) {
//        // Have each task set throw a SparkException with the error
//        for ((taskSetId, manager) <- activeTaskSets) {
//          try {
//            manager.abort(message)
//          } catch {
//            case e: Exception => logError("Exception in error callback", e)
//          }
//        }
//      } else {
//        // No task sets are active but we still got an error. Just exit since this
//        // must mean the error is during registration.
//        // It might be good to do something smarter here in the future.
//        logError("Exiting due to error from cluster scheduler: " + message)
//        System.exit(1)
//      }
//    }
//  }
//
//  override def stop() {
//    if (backend != null) {
//      backend.stop()
//    }
//    if (taskResultGetter != null) {
//      taskResultGetter.stop()
//    }
//    starvationTimer.cancel()
//
//    // sleeping for an arbitrary 1 seconds to ensure that messages are sent out.
//    Thread.sleep(1000L)
//  }
//
//  override def defaultParallelism() = backend.defaultParallelism()
//
//  // Check for speculatable tasks in all our active jobs.
//
//
//
//
//  /** Remove an executor from all our data structures and mark it as lost */
//  private def removeExecutor(executorId: String) {
//    activeExecutorIds -= executorId
//    val host = executorIdToHost(executorId)
//    val execs = executorsByHost.getOrElse(host, new HashSet)
//    execs -= executorId
//    if (execs.isEmpty) {
//      executorsByHost -= host
//      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
//        hosts -= host
//        if (hosts.isEmpty) {
//          hostsByRack -= rack
//        }
//      }
//    }
//    executorIdToHost -= executorId
//    rootPool.executorLost(executorId, host)
//  }


  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    executorsByHost.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    executorsByHost.contains(host)
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.contains(rack)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    activeExecutorIds.contains(execId)
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None


}
