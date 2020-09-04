package ai.tripl.arc.jupyter

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent._

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

  val rateLimitMs = 500

  val numTasks = new AtomicInteger
  val startedTasks = new AtomicInteger
  val doneTasks = new AtomicInteger

  val numTasksPrev = new AtomicInteger
  val startedTasksPrev = new AtomicInteger
  val doneTasksPrev = new AtomicInteger

  var scheduledFuture: ScheduledFuture[_] = _

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    numTasks.addAndGet(stageSubmitted.stageInfo.numTasks)
    logger.debug()
      .message("onStageSubmitted")
      .field("stageId", java.lang.Integer.valueOf(stageSubmitted.stageInfo.stageId))
      .field("numTasks", stageSubmitted.stageInfo.numTasks)
      .field("numTasksAccumulator", java.lang.Integer.valueOf(numTasks.get))
      .field("startedTasksAccumulator", java.lang.Integer.valueOf(startedTasks.get))
      .field("doneTasksAccumulator", java.lang.Integer.valueOf(doneTasks.get))
      .log()
    // rateLimitedUpdate
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logger.debug()
      .message("onStageCompleted")
      .field("stageId", java.lang.Integer.valueOf(stageCompleted.stageInfo.stageId))
      .field("numTasksAccumulator", java.lang.Integer.valueOf(numTasks.get))
      .field("startedTasksAccumulator", java.lang.Integer.valueOf(startedTasks.get))
      .field("doneTasksAccumulator", java.lang.Integer.valueOf(doneTasks.get))
      .log()
    // rateLimitedUpdate
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    startedTasks.incrementAndGet
    logger.debug()
      .message("taskStart")
      .field("stageId", java.lang.Integer.valueOf(taskStart.stageId))
      .field("taskId", java.lang.Long.valueOf(taskStart.taskInfo.taskId))
      .field("numTasksAccumulator", java.lang.Integer.valueOf(numTasks.get))
      .field("startedTasksAccumulator", java.lang.Integer.valueOf(startedTasks.get))
      .field("doneTasksAccumulator", java.lang.Integer.valueOf(doneTasks.get))
      .log()
    // rateLimitedUpdate
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    doneTasks.incrementAndGet
    logger.debug()
      .message("taskEnd")
      .field("stageId", java.lang.Integer.valueOf(taskEnd.stageId))
      .field("taskId", java.lang.Long.valueOf(taskEnd.taskInfo.taskId))
      .field("numTasksAccumulator", java.lang.Integer.valueOf(numTasks.get))
      .field("startedTasksAccumulator", java.lang.Integer.valueOf(startedTasks.get))
      .field("doneTasksAccumulator", java.lang.Integer.valueOf(doneTasks.get))
      .log()
    // rateLimitedUpdate
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    logger.debug().message("onJobStart").field("jobId", java.lang.Integer.valueOf(jobStart.jobId)).log()
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.debug().message("onJobEnd").field("jobId", java.lang.Integer.valueOf(jobEnd.jobId)).log()
  }

  def init()(implicit outputHandler: OutputHandler): Unit = {
    outputHandler.html(
      s"""<div class="progress"><div class="progress-bar-status">0/0</div></div>""".stripMargin,
      progressBarId
    )

    // create an interval based update
    // rate limiting was introduced as the UI updates started to hang if updates were too frequent
    val executor = new ScheduledThreadPoolExecutor(1)
    val task = new Runnable {
      def run() = update(false, false)
    }
    scheduledFuture = executor.scheduleAtFixedRate(task, rateLimitMs, rateLimitMs, TimeUnit.MILLISECONDS)
  }

  def update(error: Boolean, removeListener: Boolean)(implicit outputHandler: OutputHandler) = {
    var numTasksSnapshot = numTasks.get
    var doneTasksSnapshot = doneTasks.get
    var startedTasksSnapshot = startedTasks.get

    var numTasksPrevSnapshot = numTasksPrev.get
    var doneTasksPrevSnapshot = doneTasksPrev.get
    var startedTasksPrevSnapshot = startedTasksPrev.get

    // simple comparison to reduce unnescesssary updates
    var update = false
    if (numTasksSnapshot != numTasksPrevSnapshot) {
      numTasksPrev.set(numTasksSnapshot)
      update = true
    }
    if (doneTasksSnapshot != doneTasksPrevSnapshot) {
      doneTasksPrev.set(doneTasksSnapshot)
      update = true
    }
    if (startedTasksSnapshot != startedTasksPrevSnapshot) {
      startedTasksPrev.set(startedTasksSnapshot)
      update = true
    }

    if (update || removeListener) {
      logger.debug().message("update").log()

      if (removeListener) {
        doneTasksSnapshot = numTasksSnapshot
        startedTasksSnapshot = 0
        Option(scheduledFuture) match {
          case Some(scheduledFuture) => scheduledFuture.cancel(false)
          case None =>
        }
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
                |  <div class="progress-bar-danger" style="width: 100%;"></div>
                |  <div class="progress-bar-status">${statusText}</div>
                |</div>
                |""".stripMargin,
            progressBarId
          )
        } else {
          outputHandler.updateHtml(
            s"""<div class="progress">
                |  <div class="progress-bar-success" style="width: 100%;"></div>
                |  <div class="progress-bar-status">${statusText}</div>
                |</div>
                |""".stripMargin,
            progressBarId
          )
        }
      } else {
        outputHandler.updateHtml(
          s"""<div class="progress">
              |  <div class="progress-bar-success" style="width: ${if (!donePct.isNaN) donePct else 100}%;"></div>
              |  <div class="progress-bar-info" style="width: ${if (!runningPct.isNaN) runningPct else 0}%;"></div>
              |  <div class="progress-bar-status">${statusText}</div>
              |</div>
              |""".stripMargin,
          progressBarId
        )
      }
    }
  }
}