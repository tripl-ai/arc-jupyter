package ai.tripl.arc.jupyter

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import almond.interpreter.api.{CommHandler, CommTarget, OutputHandler}
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import ai.tripl.arc.util.log.logger.Logger

import scala.compat.Platform.currentTime
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

import scala.collection.JavaConverters._
import scala.util.Try

final class ProgressSparkListener(executionId: String, isJupyterLab: Boolean)(implicit outputHandler: OutputHandler, logger: Logger) extends SparkListener {

  val rateLimit = Duration(200, MILLISECONDS).toMillis
  var isRunning = new AtomicBoolean(false)
  var lastStopTime = Long.MinValue

  val numTasks = new AtomicInteger
  val startedTasks = new AtomicInteger
  val doneTasks = new AtomicInteger

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    logger.debug().message("onStageSubmitted").field("stageId", java.lang.Integer.valueOf(stageSubmitted.stageInfo.stageId)).field("numTasks", stageSubmitted.stageInfo.numTasks).log()
    numTasks.addAndGet(stageSubmitted.stageInfo.numTasks)
    rateLimitedUpdate
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logger.debug().message("onStageCompleted").field("stageId", java.lang.Integer.valueOf(stageCompleted.stageInfo.stageId)).log()
    rateLimitedUpdate
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    logger.debug().message("taskStart").field("stageId", java.lang.Integer.valueOf(taskStart.stageId)).field("taskId", java.lang.Long.valueOf(taskStart.taskInfo.taskId)).log()
    startedTasks.incrementAndGet
    rateLimitedUpdate
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    logger.debug().message("taskEnd").field("stageId", java.lang.Integer.valueOf(taskEnd.stageId)).field("taskId", java.lang.Long.valueOf(taskEnd.taskInfo.taskId)).log()
    doneTasks.incrementAndGet
    rateLimitedUpdate
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    logger.debug().message("onJobStart").field("jobId", java.lang.Integer.valueOf(jobStart.jobId)).log()
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.debug().message("onJobEnd").field("jobId", java.lang.Integer.valueOf(jobEnd.jobId)).log()
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
    update(true, false)
  }

  def update(error: Boolean, removeListener: Boolean)(implicit outputHandler: OutputHandler) = {
    var numTasksSnapshot = numTasks.get
    var doneTasksSnapshot = doneTasks.get
    var startedTasksSnapshot = startedTasks.get

    if (removeListener) {
      doneTasksSnapshot = numTasksSnapshot
      startedTasksSnapshot = 0
    }

    val runningTasks = Math.max(startedTasksSnapshot - doneTasksSnapshot, 0)
    val donePct = Math.min(100.0 * doneTasksSnapshot.toDouble / numTasksSnapshot.toDouble, 100.0)
    val runningPct = Math.min(100.0 * runningTasks.toDouble / numTasksSnapshot.toDouble, 100.0)

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

    // jupyterlab has a different format
    if (isJupyterLab) {
      if (removeListener) {
        if (error) {
          outputHandler.updateHtml(
            s"""<div class="progress">
                |  <div class="progress-bar-danger" style="width: $donePct%;">${statusText}</div>
                |</div>
                |""".stripMargin,
            executionId
          )
        } else {
          outputHandler.updateHtml(
            s"""<div class="progress">
                |  <div class="progress-bar-success" style="width: $donePct%;">${statusText}</div>
                |</div>
                |""".stripMargin,
            executionId
          )
        }
      } else {
        outputHandler.updateHtml(
          s"""<div class="progress">
              |  <div class="progress-bar-info" style="width: $donePct%;">${statusText}</div>
              |</div>
              |""".stripMargin,
          executionId
        )
      }
    } else {
      outputHandler.updateHtml(
        s"""<div class="progress arc-background">
            |  <div class="progress-bar arc-complete ${statusClass}" style="width: $donePct%;">${statusText}</div>
            |  <div class="progress-bar arc-running ${statusClass}" style="width: $runningPct%;"></div>
            |</div>
            |""".stripMargin,
        executionId
      )
    }
  }

  // rate limiting was introduced as the UI updates started to hang if updates were too frequent
  def rateLimitedUpdate()(implicit outputHandler: OutputHandler) = {
    val doneWaiting = lastStopTime + rateLimit <= currentTime
    if (isRunning.compareAndSet(false, doneWaiting) && doneWaiting) {
      try {
        update(false, false)
      } finally {
        lastStopTime = currentTime
        isRunning.set(false)
      }
    }
  }

}