package ai.tripl.arc.jupyter

import java.security.SecureRandom

import util.control.Breaks._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import almond.interpreter.api.{DisplayData, OutputHandler}
import almond.interpreter.{Completion, ExecuteResult, Inspection, Interpreter}

import ai.tripl.arc.api.API
import ai.tripl.arc.api.API._

object Common {

  // converts the schema of an input dataframe into a dataframe [name, nullable, type, metadata.*]
  def createPrettyMetadataDataframe(input: DataFrame)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): DataFrame = {
    import spark.implicits._

    // this is a hack but having to create a StructType using union of all metadata maps is not trivial
    val schemaDataframe = spark.sparkContext.parallelize(Seq(input.schema.json)).toDF.as[String]
    val parsedSchema = spark.read.json(schemaDataframe)

    // create schema dataframe
    val schemaDF = parsedSchema.select(explode(col("fields"))).select("col.*")

    // add metadata column if missing
    val output = if (schemaDF.columns.contains("metadata")) {
      val nonMetadataFields = schemaDF.schema.fields.filter { field => field.name != "metadata" }.map { field => col(field.name) }
      // explode the metadata column for readability
      val metadataFields = schemaDF.schema.fields(schemaDF.schema.fieldIndex("metadata")).dataType.asInstanceOf[StructType].fields.map {
        field => col(s"metadata.${field.name}").as(s"metadata.${field.name}")
      }
      schemaDF.select((nonMetadataFields ++ metadataFields):_*)
    } else {
      schemaDF.withColumn("metadata", typedLit(Map[String, String]()))
    }

    output.cache.count
    output
  }

  def getHelp(): String = {
    """
    |Commands:
    |%arc
    |Run the stage as an Arc stage. Useful if you want to override the config settings for an individual cell.
    |Supported configuration parameters: numRows, truncate, monospace, streamingDuration
    |
    |%sql
    |Run an inline SQLTransform stage. e.g.:
    |%sql name="calculate weather" outputView=weather environments=production,test persist=true
    |
    |%sqlvalidate
    |Run an inline SQLValidate stage. e.g.:
    |%sqlvalidate name="ensure no errors exist after data typing" environments=production,test sqlParams=input_table=${ETL_CONF_TABLE}
    |
    |%cypher
    |Run a Cypher graph query. Scala 2.12 only.
    |Supported configuration parameters: numRows, truncate, outputView, persist, monospace
    |
    |%schema [view]
    |Display a JSON formatted schema for the input view
    |
    |%printschema [view]
    |Display a printable basic schema for the input view
    |
    |%metadata [view]
    |Create an Arc metadata dataset for the input view
    |Supported configuration parameters: numRows, truncate, outputView, persist, monospace
    |
    |%printmetadata [view]
    |Display a JSON formatted Arc metadata schema for the input view
    |
    |%summary [view]
    |Create an Summary statistics dataset for the input view
    |Supported configuration parameters: numRows, truncate, outputView, persist, monospace
    |
    |%env
    |Set variables for this session. E.g. ETL_CONF_BASE_DIR=/home/jovyan/tutorial
    |Supported configuration parameters: numRows, truncate, outputView, persist, monospace, streamingDuration
    |
    |%secret
    |Set secrets for this session. E.g. ETL_CONF_SECRET
    |
    |%conf
    |Set global Configuration Parameters which will apply to all cells.
    |
    |%help
    |Display this help text.
    |
    |Configuration Parameters:
    |master:            The address of the Spark master (if connecting to a remote cluster)
    |streaming:         Set the notebook into streaming mode (boolean)
    |streamingDuration: How many seconds to execute a streaming stage before stopping (will stop if numRows is reached first).
    |
    |Display Parameters:
    |datasetLabels:     Display labels with the name of the registered table name
    |leftAlign:         Left-align output datasets
    |monospace:         Use a fixed-width font
    |numRows:           The maximum number of rows to return in a dataset (integer)
    |truncate:          The maximum number of characters displayed in a string result (integer)
    """.stripMargin
  }

  def getVersion()(implicit spark: SparkSession, arcContext: ARCContext): String = {
    s"""
    |spark: ${spark.version}
    |arc: ${ai.tripl.arc.ArcBuildInfo.BuildInfo.version}
    |arc-jupyter: ${ai.tripl.arc.jupyter.BuildInfo.version}
    |scala: ${scala.util.Properties.versionNumberString}
    |java: ${System.getProperty("java.runtime.version")}
    |dynamicConfigurationPlugins:
    |${arcContext.dynamicConfigurationPlugins.map(c => s" - ${c.getClass.getName}:${c.version}").sorted.mkString("\n")}
    |pipelinePlugins:
    |${arcContext.pipelineStagePlugins.map(c => s" - ${c.getClass.getName}:${c.version}").sorted.mkString("\n")}
    |udfPlugins:
    |${arcContext.udfPlugins.map(c => s" - ${c.getClass.getName}:${c.version}").sorted.mkString("\n")}
    """.stripMargin
  }

  def injectParameters(sql: String, params: Map[String, String])(implicit logger: ai.tripl.arc.util.log.logger.Logger): String = {
    // replace sqlParams parameters
    var stmt = params.foldLeft(sql) {
      case (stmt, (k,v)) => {
        val matchPlaceholderRegex = "[$][{]\\s*" + k + "\\s*(?:=[^}]+)?[}]"
        matchPlaceholderRegex.r.replaceAllIn(stmt, v)
      }
    }
    stmt
  }

  def renderResult(spark: SparkSession, outputHandler: Option[OutputHandler], stage: Option[PipelineStage], df: DataFrame, numRows: Int, truncate: Int, monospace: Boolean, leftAlign: Boolean, datasetLabels: Boolean, streamingDuration: Int, confStreamingFrequency: Int) = {
    if (!df.isStreaming) {
      ExecuteResult.Success(
        DisplayData.html(renderHTML(df, stage, numRows, truncate, monospace, leftAlign, datasetLabels))
      )
    } else {
      outputHandler match {
        case Some(outputHandler) => {
          // create a random name for the element to update
          val outputElementHandle = randStr(32)

          // create a random name for the stream
          val queryName = randStr(32)

          // start a stream
          val writeStream = df.writeStream
            .format("memory")
            .outputMode("append")
            .queryName(queryName)
            .start

          // periodically update results on screen
          val endTime = System.currentTimeMillis + (streamingDuration * 1000)
          var initial = true

          breakable {
            while (System.currentTimeMillis <= endTime) {

              val df = spark.table(queryName)
              df.persist

              val count = df.count
              // create the html handle on the first run
              if (initial) {
                outputHandler.html(
                  renderHTML(df, None, numRows, truncate, monospace, leftAlign, datasetLabels),
                  outputElementHandle
                )
                initial = false
              } else {
                outputHandler.updateHtml(
                  renderHTML(df, None, numRows, truncate, monospace, leftAlign, datasetLabels),
                  outputElementHandle
                )
              }

              df.unpersist

              if (count > numRows) {
                break
              }
              Thread.sleep(confStreamingFrequency)
            }
          }

          // stop stream and display final result
          writeStream.stop
          outputHandler.html("", outputElementHandle)
          ExecuteResult.Success(
            DisplayData.html(renderHTML(spark.table(queryName), None, numRows, truncate, monospace, leftAlign, datasetLabels))
          )
        }
        case None => ExecuteResult.Error("No result.")
      }
    }
  }

  def renderHTML(df: DataFrame, stage: Option[PipelineStage], numRows: Int, truncate: Int, monospace: Boolean, leftAlign: Boolean, datasetLabels: Boolean): String = {
    import xml.Utility.escape

    val header = df.columns

    // add index to all the column names so they are unique
    val renamedDF = df.toDF(df.columns.zipWithIndex.map { case (col, idx) => s"${col}${idx}" }:_*)

    // this code has come from the spark Dataset class:
    val castCols = renamedDF.schema.map { field =>
      // explicitly wrap names to fix any nested select problems
      val fieldName = s"`${field.name}`"

      // Since binary types in top-level schema fields have a specific format to print,
      // so we do not cast them to strings here.
      field.dataType match {
        case BinaryType => col(fieldName)
        // replace commas (from format_number), replace any trailing zeros (but leave at least one character after the .)
        case DoubleType => regexp_replace(regexp_replace(regexp_replace(format_number(col(fieldName), 10),",",""),"(?<=.[0-9]{2})0+$",""),"^\\.","0.")
        case x: DecimalType => regexp_replace(format_number(col(fieldName), x.scale),",","")
        case _ => col(fieldName).cast(StringType)
      }
    }
    val data = renamedDF.select(castCols: _*).take(numRows)

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond `truncate` characters, replace it with the
    // first `truncate-3` and "..."
    val rows = data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }: Seq[String]
    }

    val monospaceClass = if (monospace) "monospace" else ""
    val leftAlignClass = if (leftAlign) "leftalign" else ""

    // add dataset name if exists
    val label = if (datasetLabels) {
      val (inputView, outputView) = stage match {
        case Some(stage) => 
          val inputView = try {
            stage.getClass.getMethod("inputView").invoke(stage) match {
              case Some(stageName: String) => Option(stageName)
              case stageName: String => Option(stageName)
              case _ => None
            }
          } catch {
            case e: Exception => None
          }         
          val outputView = try {
            stage.getClass.getMethod("outputView").invoke(stage) match {
              case Some(stageName: String) => Option(stageName)
              case stageName: String => Option(stageName)
              case _ => None
            }
          } catch {
            case e: Exception => None
          }
          (inputView, outputView)
        case None => ""
      }
      (inputView, outputView) match {
        case (_, Some(ouputView)) => s"""<div class="table_name ${if (monospace) "monospace" else ""}">${ouputView}</div>"""
        case (Some(inputView), _) => s"""<div class="table_name ${if (monospace) "monospace" else ""}">${inputView}</div>"""
        case _ => ""
      }
    } else {
      ""
    }

    s"""${label}<table class="tex2jax_ignore ${monospaceClass} ${leftAlignClass}"><thead><tr>${header.map(h => s"<th>${escape(h)}</th>").mkString}</tr></thead><tbody>${rows.map { row => s"<tr>${row.map { cell => s"<td>${escape(cell)}</td>" }.mkString}</tr>"}.mkString}</tbody></table>"""
  }

  val alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val size = alpha.size
  val secureRandom = new SecureRandom
  def randStr(n:Int) = (1 to n).map(x => alpha(secureRandom.nextInt.abs % size)).mkString

}