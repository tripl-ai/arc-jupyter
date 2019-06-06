package ai.tripl.arc.jupyter

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import almond.interpreter.api.{CommHandler, CommTarget, OutputHandler}
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession

import scala.compat.Platform.currentTime
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

import scala.collection.JavaConverters._
import scala.util.Try

final class ProgressSparkListener(executionId: String)(implicit outputHandler: OutputHandler) extends SparkListener {

  val rateLimit = Duration(200, MILLISECONDS).toMillis
  var isRunning = new AtomicBoolean(false)
  var lastStopTime = Long.MinValue

  val numTasks = new AtomicInteger
  val startedTasks = new AtomicInteger
  val doneTasks = new AtomicInteger

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    numTasks.addAndGet(stageSubmitted.stageInfo.numTasks)
    update(false)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    update(false)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    startedTasks.incrementAndGet
    rateLimitedUpdate
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    doneTasks.incrementAndGet
    rateLimitedUpdate
  }

  def init()(implicit outputHandler: OutputHandler): Unit = {
    outputHandler.html(
      s"""<div class="progress arc-background">
        |  <div class="progress-bar arc-complete" style="width: 0%;">0 / 0</div>
        |</div>
        |""".stripMargin,
      executionId
    )
  }  

  def error()(implicit outputHandler: OutputHandler): Unit = {  
    update(true)
  }   

  def update(error: Boolean)(implicit outputHandler: OutputHandler) = {
    val numTasksSnapshot = numTasks.get
    val doneTasksSnapshot = doneTasks.get
    val startedTasksSnapshot = startedTasks.get

    val runningTasks = startedTasksSnapshot - doneTasksSnapshot
    val donePct = 100.0 * doneTasksSnapshot.toDouble / numTasksSnapshot.toDouble
    val runningPct = 100.0 * runningTasks.toDouble / numTasksSnapshot.toDouble

    val statusText = if (doneTasksSnapshot == numTasksSnapshot || error) {
      s"${doneTasksSnapshot}/${numTasksSnapshot}"
    } else {
      s"${doneTasksSnapshot}/${numTasksSnapshot} (${runningTasks} running)"
    }

    val statusClass = if (error) {
      "error"
    } else {
      ""
    }

    outputHandler.updateHtml(
      s"""<div class="progress arc-background">
          |  <div class="progress-bar arc-complete ${statusClass}" style="width: $donePct%;">${statusText}</div>
          |  <div class="progress-bar arc-running ${statusClass}" style="width: $runningPct%;"></div>
          |</div>
          |""".stripMargin,
      executionId
    )
  }    

  def rateLimitedUpdate()(implicit outputHandler: OutputHandler) = {
    val doneWaiting = lastStopTime + rateLimit <= currentTime
    if (isRunning.compareAndSet(false, doneWaiting) && doneWaiting) {
      try {
        update(false)
      } finally {
        lastStopTime = currentTime
        isRunning.set(false)
      }
    }
  }  

}