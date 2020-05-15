package ai.tripl.arc.jupyter

import java.security.SecureRandom

import org.apache.spark.sql.{DataFrame, SparkSession}

import ai.tripl.arc.api.API._
import ai.tripl.arc.plugins.LifecyclePlugin
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.Utils
import ai.tripl.arc.config.Error._
import almond.interpreter.api.{DisplayData, OutputHandler}

class OutputTable extends LifecyclePlugin {

  val version = "0.0.1"

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], LifecyclePluginInstance] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "numRows" :: "truncate" :: "monospace" :: "leftAlign" :: "datasetLabels" :: Nil
    val numRows = getValue[Int]("numRows")
    val truncate = getValue[Int]("truncate")
    val monospace = getValue[java.lang.Boolean]("monospace")
    val leftAlign = getValue[java.lang.Boolean]("leftAlign")
    val datasetLabels = getValue[java.lang.Boolean]("datasetLabels")
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (numRows, truncate, monospace, leftAlign, datasetLabels, invalidKeys) match {
      case (Right(numRows), Right(truncate), Right(monospace), Right(leftAlign), Right(datasetLabels), Right(invalidKeys)) =>
        Right(OutputTablePlugin(
          plugin=this,
          numRows=numRows,
          truncate=truncate,
          monospace=monospace,
          leftAlign=leftAlign,
          datasetLabels=datasetLabels
        ))
      case _ =>
        val allErrors: Errors = List(numRows, truncate, monospace, leftAlign, datasetLabels, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val err = StageError(index, this.getClass.getName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class OutputTablePlugin (
    plugin: OutputTable,
    numRows: Int,
    truncate: Int,
    monospace: Boolean,
    leftAlign: Boolean,
    datasetLabels: Boolean
  ) extends LifecyclePluginInstance {

  override def after(result: Option[DataFrame], stage: PipelineStage, index: Int, stages: List[PipelineStage])(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    // do not print the last stage as it will be printed by the interpreter
    val isLast = index == stages.length - 1
    if (!isLast) {
      result match {
        case Some(df) => {
          val outputHandler = arcContext.userData.get("outputHandler") match {
            case Some(outputHandler: OutputHandler) => {
              outputHandler.asInstanceOf[OutputHandler].html(
                Common.renderHTML(df, Some(stage), numRows, truncate, monospace, leftAlign, datasetLabels),
                Common.randStr(32)
              )
            }
            case _ =>
          }
        }
        case None =>
      }
    }

    result
  }

}
