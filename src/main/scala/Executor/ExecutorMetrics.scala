package main.scala.Executor

/**
 * Created by shivinkapur on 3/12/15.
 */

import java.io.Serializable
import scala.collection.mutable.ArrayBuffer

class ExecutorMetrics extends Serializable {
  /**
   * Total number of remaining mini-partitions
   */
  var remainingMiniPartitions: Int = _

  /**
   * List of partitionIDs remaining to be processed (candidates to be stolen)
   */
  var remainingPartitionIds: ArrayBuffer[Int] = new ArrayBuffer[Int]()

  /**
   * Number of partitions successfully processed so far
   */
  var processedMiniPartitions: Int = _

  /**
   * List of partitionIDs that have been successfully processed so far
   */
  var processedPartitionIds: ArrayBuffer[Int] = new ArrayBuffer[Int]()

  var avgPartitionsProcessedPerHeartbeat: Double = _

  var partitionsProcessedSinceLastHeartbeat: Int = _

  /**
   * Number of partitions stolen FROM this iterator so far
   */
  var partitionsStolenFromSoFar: Int = _

  /**
   * Number of partitions stolen BY this iterator so far
   */
  var partitionsStolenBySoFar: Int = _

  /**
   * Dummy record for testing purpose
   */
  var timestap: Long = _
}