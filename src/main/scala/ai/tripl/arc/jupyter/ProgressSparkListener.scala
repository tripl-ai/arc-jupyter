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

final class ProgressSparkListener(progressBarId: String)(implicit outputHandler: OutputHandler, logger: Logger) extends SparkListener {

  val rateLimit = Duration(150, MILLISECONDS).toMillis
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
      s"""<div class="progress">0/0</div>""",
      progressBarId
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

    if (removeListener) {
      if (error) {
        outputHandler.updateHtml(
          s"""<div class="progress">
              |  <div class="progress-bar-danger" style="width: 100%;">${statusText}</div>
              |</div>
              |""".stripMargin,
          progressBarId
        )
      } else {
        outputHandler.updateHtml(
          s"""<div class="progress">
              |  <div class="progress-bar-success" style="width: 100%;">${statusText}</div>
              |</div>
              |""".stripMargin,
          progressBarId
        )
      }
    } else {
      outputHandler.updateHtml(
        s"""<div class="progress">
            |  <div class="progress-bar-success"" style="width: ${if (!donePct.isNaN) donePct else 100}%;">${statusText}</div>
            |  <div class="progress-bar-info" style="width: ${if (!runningPct.isNaN) runningPct else 0}%;"></div>
            |</div>
            |""".stripMargin,
        progressBarId
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