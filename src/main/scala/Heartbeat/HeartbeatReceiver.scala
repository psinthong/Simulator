package main.scala.Heartbeat

import main.scala.Scheduler.TaskScheduler
import main.scala.Executor.TaskMetrics

/**
 * Created by shivinkapur on 3/13/15.
 */

/**
 * A heartbeat from executors to the driver. This is a shared message used by several internal
 * components to convey liveness or execution information for in-progress tasks.
 */
private case class Heartbeat(executorId: String,
                             taskMetrics: Array[(Long, TaskMetrics)] // taskId -> TaskMetrics
                             )

private case class HeartbeatResponse()

/**
 * Lives in the driver to receive heartbeats from executors..
 */
private class HeartbeatReceiver(scheduler: TaskScheduler) {

//  override def receiveWithLogging = {
//    case Heartbeat(executorId, taskMetrics) =>
//      val response = HeartbeatResponse(
//        !scheduler.executorHeartbeatReceived(executorId, taskMetrics))
//      sender ! response
//  }
}