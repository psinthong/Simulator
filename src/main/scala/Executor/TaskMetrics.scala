package main.scala.Executor

/**
 * Created by shivinkapur on 3/13/15.
 */

import main.scala.Executor.ExecutorMetrics

import scala.collection.mutable.ArrayBuffer

/**

import org.apache.spark.executor.{MultiIteratorMetrics, ExecutorMetrics}
 during the execution of a task.
  *
  * This class is used to house metrics both for in-progress and completed tasks. In executors,
  * both the task thread and the heartbeat thread write to the TaskMetrics. The heartbeat thread
  * reads it to send in-progress metrics, and the task thread reads it to send metrics along with
  * the completed task.
  *
  * So, when adding new fields, take into consideration that the whole object can be serialized for
  * shipping off at any time to consumers of the SparkListener interface.
  */
class TaskMetrics extends Serializable {
  /**
   * Host's name the task runs on
   */
  var hostname: String = _

  /**
   * Time taken on the executor to deserialize this task
   */
  var executorDeserializeTime: Long = _

  /**
   * Time the executor spends actually running the task (including fetching shuffle data)
   */
  var executorRunTime: Long = _

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult
   */
  var resultSize: Long = _

  /**
   * Amount of time the JVM spent in garbage collection while executing this task
   */
  var jvmGCTime: Long = _

  /**
   * Amount of time spent serializing the task result
   */
  var resultSerializationTime: Long = _

  /**
   * The number of in-memory bytes spilled by this task
   */
  var memoryBytesSpilled: Long = _

  /**
   * The number of on-disk bytes spilled by this task
   */
  var diskBytesSpilled: Long = _

  /**
   * If this task reads from a HadoopRDD or from persisted data, metrics on how much data was read
   * are stored here.
   */
  var inputMetrics: Option[InputMetrics] = None

  /**
   * If this task reads from shuffle output, metrics on getting shuffle data will be collected here.
   * This includes read metrics aggregated over all the task's shuffle dependencies.
   */
  private var _shuffleReadMetrics: Option[ShuffleReadMetrics] = None

  def shuffleReadMetrics = _shuffleReadMetrics

  /**
   * This should only be used when recreating TaskMetrics, not when updating read metrics in
   * executors.
   */
  private def setShuffleReadMetrics(shuffleReadMetrics: Option[ShuffleReadMetrics]) {
    _shuffleReadMetrics = shuffleReadMetrics
  }

  /**
   * ShuffleReadMetrics per dependency for collecting independently while task is in progress.
   */
  @transient private lazy val depsShuffleReadMetrics: ArrayBuffer[ShuffleReadMetrics] =
    new ArrayBuffer[ShuffleReadMetrics]()

  /**
   * If this task writes to shuffle output, metrics on the written shuffle data will be collected
   * here
   */
  var shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None

  var executorMetrics: Option[ExecutorMetrics] = None

  @transient private lazy val executorMetricsHistory: ArrayBuffer[ExecutorMetrics] =
    new ArrayBuffer[ExecutorMetrics]()

  /**
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a ShuffleReadMetrics for each
   * dependency, and merge these metrics before reporting them to the driver. This method returns
   * a ShuffleReadMetrics for a dependency and registers it for merging later.
   */
  private def createShuffleReadMetricsForDependency(): ShuffleReadMetrics = synchronized {
    val readMetrics = new ShuffleReadMetrics()
    depsShuffleReadMetrics += readMetrics
    readMetrics
  }

  private def createExecutorMetrics(): ExecutorMetrics = synchronized {
    val newMetrics = new ExecutorMetrics()
    executorMetricsHistory += newMetrics
    newMetrics
  }

  /**
   * Aggregates shuffle read metrics for all registered dependencies into shuffleReadMetrics.
   */
  private def updateShuffleReadMetrics() = synchronized {
    val merged = new ShuffleReadMetrics()
    for (depMetrics <- depsShuffleReadMetrics) {
      merged.fetchWaitTime += depMetrics.fetchWaitTime
      merged.localBlocksFetched += depMetrics.localBlocksFetched
      merged.remoteBlocksFetched += depMetrics.remoteBlocksFetched
      merged.remoteBytesRead += depMetrics.remoteBytesRead
      merged.shuffleFinishTime = math.max(merged.shuffleFinishTime, depMetrics.shuffleFinishTime)
//      merged.multiIteratorMetrics = depMetrics.multiIteratorMetrics
    }
    _shuffleReadMetrics = Some(merged)
  }

  /**
   * Aggregates shuffle read metrics for all registered dependencies into shuffleReadMetrics.
   */

  private def updateExecutorMetrics() = synchronized {
    executorMetrics = executorMetricsHistory.headOption
  }

  private def setExecutorMetrics(em: ExecutorMetrics): Unit = {
    executorMetricsHistory += em
    executorMetrics = Some(em)
  }
}

private object TaskMetrics {
  def empty: TaskMetrics = new TaskMetrics
}

/**
 * :: DeveloperApi ::
 * Method by which input data was read.  Network means that the data was read over the network
 * from a remote block manager (which may have stored the data on-disk or in-memory).
 */
//@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  val Memory, Disk, Hadoop, Network = Value
}

/**
 * :: DeveloperApi ::
 * Metrics about reading input data.
 */
//@DeveloperApi
case class InputMetrics(readMethod: DataReadMethod.Value) {
  /**
   * Total bytes read.
   */
  var bytesRead: Long = 0L
}


/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data read in a given task.
 */
//@DeveloperApi
class ShuffleReadMetrics extends Serializable {
  /**
   * Absolute time when this task finished reading shuffle data
   */
  var shuffleFinishTime: Long = -1

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local)
   */
  def totalBlocksFetched: Int = remoteBlocksFetched + localBlocksFetched

  /**
   * Number of remote blocks fetched in this shuffle by this task
   */
  var remoteBlocksFetched: Int = _

  /**
   * Number of local blocks fetched in this shuffle by this task
   */
  var localBlocksFetched: Int = _

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  var fetchWaitTime: Long = _

  /**
   * Total number of remote bytes read from the shuffle by this task
   */
  var remoteBytesRead: Long = _

}

/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data written in a given task.
 */
//@DeveloperApi
class ShuffleWriteMetrics extends Serializable {
  /**
   * Number of bytes written for the shuffle by this task
   */
  @volatile var shuffleBytesWritten: Long = _

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds
   */
  @volatile var shuffleWriteTime: Long = _
}
