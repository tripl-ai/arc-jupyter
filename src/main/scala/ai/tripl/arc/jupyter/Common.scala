package ai.tripl.arc.jupyter

import java.security.SecureRandom
import java.lang.management.ManagementFactory
import java.io._
import java.util.Properties
import java.nio.charset.StandardCharsets

import util.control.Breaks._
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}

import almond.interpreter.api.{DisplayData, OutputHandler}
import almond.interpreter.{Completion, ExecuteResult, Inspection, Interpreter}
import almond.protocol.RawJson

import ai.tripl.arc.api.API
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.log.LoggerFactory

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._

import org.apache.log4j.{Level, Logger}

object Common {
  val TABLE_COMPLETIONS_KEY = "ai.tripl.arc.jupyter.tableCompletions"

  case class ConfigValue (
    secret: Boolean,
    value: String
  )

  val objectMapper = new ObjectMapper()

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
    |
    |%conf
    |Set global Configuration Parameters which will apply to all cells.
    |
    |%env
    |Set variables for this session. E.g. ETL_CONF_BASE_DIR=/home/jovyan/tutorial
    |
    |%help
    |Display this help text.
    |
    |%list
    |Show a list of files in a target directory
    |
    |%metadata
    |[view]
    |Create an Arc metadata dataset for the input view
    |
    |%printmetadata
    |[view]
    |Display a JSON formatted Arc metadata schema for the input view
    |
    |%printschema
    |[view]
    |Display a printable basic schema for the input view
    |
    |%schema
    |[view]
    |Display a JSON formatted schema for the input view
    |
    |%secret
    |Set secrets for this session. E.g. ETL_CONF_SECRET
    |
    |%version
    |Display Arc and Arc-Jupyter version information.
    |
    |
    |Configuration Parameters:
    |master:            The address of the Spark master (if connecting to a remote cluster)
    |streaming:         Set the notebook into streaming mode (boolean)
    |streamingDuration: How many seconds to execute a streaming stage before stopping (will stop if numRows is reached first).
    |
    |Display Parameters:
    |datasetLabels:     Display labels with the name of the registered table name
    |extendedErrors:    Show more verbose errors.
    |leftAlign:         Left-align output datasets
    |logger:            Show Arc logs as part of the result set.
    |monospace:         Use a fixed-width font
    |numRows:           The maximum number of rows to return in a dataset (integer)
    |truncate:          The maximum number of characters displayed in a string result (integer)
    """.stripMargin
  }

  def getVersion()(implicit spark: SparkSession, arcContext: ARCContext): String = {
    // the memory available to the container (i.e. the docker memory limit)
    val physicalMemory = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean].getTotalPhysicalMemorySize
    // the JVM requested memory (-Xmx)
    val runtimeMemory = Runtime.getRuntime.maxMemory
    s"""
    |spark: ${spark.version}
    |arc: ${ai.tripl.arc.ArcBuildInfo.BuildInfo.version}
    |arc-jupyter: ${ai.tripl.arc.jupyter.BuildInfo.version}
    |scala: ${scala.util.Properties.versionNumberString}
    |java: ${System.getProperty("java.runtime.version")}
    |runtimeMemory: ${runtimeMemory}B
    |physicalMemory: ${physicalMemory}B
    |dynamicConfigurationPlugins:
    |${arcContext.dynamicConfigurationPlugins.map(c => s" - ${c.getClass.getName}:${c.version}").sorted.mkString("\n")}
    |pipelinePlugins:
    |${arcContext.pipelineStagePlugins.map(c => s" - ${c.getClass.getName}:${c.version}").sorted.mkString("\n")}
    |lifecyclePlugins:
    |${arcContext.lifecyclePlugins.map(c => s" - ${c.getClass.getName}:${c.version}").sorted.mkString("\n")}
    |udfPlugins:
    |${arcContext.udfPlugins.map(c => s" - ${c.getClass.getName}:${c.version}").sorted.mkString("\n")}
    """.stripMargin
  }

  def injectParameters(sql: String, params: Map[String, String]): String = {
    // replace sqlParams parameters
    var stmt = params.foldLeft(sql) {
      case (stmt, (k,v)) => {
        val matchPlaceholderRegex = "[$][{]\\s*" + k + "\\s*(?:=[^}]+)?[}]"
        matchPlaceholderRegex.r.replaceAllIn(stmt, v)
      }
    }
    stmt
  }

  def renderResult(spark: SparkSession, outputHandler: Option[OutputHandler], stage: Option[PipelineStage], df: DataFrame, inMemoryLoggerAppender: InMemoryLoggerAppender, numRows: Int, maxNumRows: Int, truncate: Int, monospace: Boolean, leftAlign: Boolean, datasetLabels: Boolean, streamingDuration: Int, confStreamingFrequency: Int, confShowLog: Boolean): ExecuteResult = {
    if (!df.isStreaming) {
      ExecuteResult.Success(
        DisplayData.html(renderHTML(df, Option(inMemoryLoggerAppender), stage, numRows, maxNumRows, truncate, monospace, leftAlign, datasetLabels, confShowLog))
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
                  renderHTML(df, None, None, numRows, maxNumRows, truncate, monospace, leftAlign, datasetLabels, confShowLog),
                  outputElementHandle
                )
                initial = false
              } else {
                outputHandler.updateHtml(
                  renderHTML(df, None, None, numRows, maxNumRows, truncate, monospace, leftAlign, datasetLabels, confShowLog),
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
            DisplayData.html(renderHTML(spark.table(queryName), Option(inMemoryLoggerAppender), None, numRows, maxNumRows, truncate, monospace, leftAlign, datasetLabels, confShowLog))
          )
        }
        case None => ExecuteResult.Error("No result.")
      }
    }
  }

  def renderText(text: String, inMemoryLoggerAppender: InMemoryLoggerAppender, confShowLog: Boolean): String = {
    val html = s"""<pre>${text}</pre>"""
    if (confShowLog) {
      val message = objectMapper.readValue(inMemoryLoggerAppender.getResult.last, classOf[java.util.HashMap[String, Object]])
      val reformatted = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(message).replaceAll("\n"," ").replaceAll("\\s\\s+", " ").replaceAll("\" :", "\":").replaceAll("\\{ ", "{").replaceAll(" \\}", "}").replaceAll("\\[ ", "[").replaceAll(" \\]", "]")
      s"""${html}<div class="log tex2jax_ignore"><div>${reformatted}</div></div>"""
    } else {
      html
    }
  }

  def renderHTML(df: DataFrame, inMemoryLoggerAppender: Option[InMemoryLoggerAppender], stage: Option[PipelineStage], numRows: Int, maxNumRows: Int, truncate: Int, monospace: Boolean, leftAlign: Boolean, datasetLabels: Boolean, confShowLog: Boolean): String = {
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
        // will convert to stringtype with UTC ZoneID
        case TimestampType => concat(col(fieldName).cast(StringType),lit("Z"))
        case DateType => date_format(col(fieldName), "yyyy-MM-dd")
        case _ => col(fieldName).cast(StringType)
      }
    }
    val data = renamedDF.select(castCols: _*).take(Math.min(numRows, maxNumRows))

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


    val table = s"""${label}<table class="tex2jax_ignore ${monospaceClass} ${leftAlignClass}"><thead><tr>${header.map(h => s"<th>${escape(h)}</th>").mkString}</tr></thead><tbody>${rows.map { row => s"<tr>${row.map { cell => s"<td>${escape(cell)}</td>" }.mkString}</tr>"}.mkString}</tbody></table>"""
    if (confShowLog && inMemoryLoggerAppender.isDefined) {
      val message = objectMapper.readValue(inMemoryLoggerAppender.get.getResult.last, classOf[java.util.HashMap[String, Object]])
      val reformatted = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(message).replaceAll("\n"," ").replaceAll("\\s\\s+", " ").replaceAll("\" :", "\":").replaceAll("\\{ ", "{").replaceAll(" \\}", "}").replaceAll("\\[ ", "[").replaceAll(" \\]", "]")
      s"""${table}<div class="log tex2jax_ignore"><div>${reformatted}</div></div>"""
    } else {
      table
    }
  }

  val alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val size = alpha.size
  val secureRandom = new SecureRandom
  def randStr(n:Int) = (1 to n).map(x => alpha(secureRandom.nextInt.abs % size)).mkString

  // calculate a list of all the fields including nested in a schema
  val notAllowed = "([^abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_])".r
  def flattenSchema(schema: StructType, parents: Seq[String] = Seq()): Seq[Seq[String]] = {
    def escape(name: String): String = {
      if (notAllowed.findFirstMatchIn(name).isEmpty) name else s"`$name`"
    }

    schema.fields.flatMap {
      case StructField(name, inner: StructType, _, _) => Seq(parents ++ Seq(escape(name))) ++ flattenSchema(inner, parents ++ Seq(escape(name)))
      case StructField(name, _, _, _) => Seq(parents ++ Seq(escape(name)))
    }
  }

  case class Completer (
    text: String,
    textType: String,
    replaceText: String,
    language: String,
    documentation: String
  )

  // memoize so as to not have to reload each time
  var pipelinePluginCompletions: List[Completer] = List.empty
  var lifecyclePluginCompletions: List[Completer] = List.empty

  def makeJupyterCompletions(completionEnvironments: String): Seq[Completer] = {
    Seq(
      Completer(
        "%log",
        "execute",
        s"""%log name="log" environments=${completionEnvironments}
        |SELECT
        |  TO_JSON(
        |    NAMED_STRUCT(
        |      'key', 'value'
        |    )
        |  ) AS message""".stripMargin,
        "sql",
        "https://arc.tripl.ai/execute/#logexecute"
      ),
      Completer(
        "%configexecute",
        "execute",
        s"""%configexecute name="configexecute" environments=${completionEnvironments}
        |SELECT
        |  TO_JSON(
        |    NAMED_STRUCT(
        |      'key', 'value'
        |    )
        |  ) AS parameters""".stripMargin,
        "sql",
        "https://arc.tripl.ai/execute/#configexecute"
      ),
      Completer(
        "%metadatafilter",
        "transform",
        s"""%metadatafilter name="metadatafiltertransform" inputView=inputView outputView=outputView environments=${completionEnvironments}
        |SELECT
        |  *
        |FROM metadata""".stripMargin,
        "sql",
        "https://arc.tripl.ai/transform/#metadatafiltertransform"
      ),
      Completer(
        "%metadatavalidate",
        "validate",
        s"""%metadatavalidate name="metadatavalidate" inputView=inputView environments=${completionEnvironments}
        |SELECT
        |  SUM(test) = 0
        |  ,TO_JSON(
        |    NAMED_STRUCT(
        |      'columns', COUNT(*),
        |      'test', SUM(test)
        |    )
        |  )
        |FROM (
        |  SELECT
        |    CASE WHEN TRUE THEN 1 ELSE 0 END AS test
        |  FROM metadata
        |) valid""".stripMargin,
        "sql",
        "https://arc.tripl.ai/validate/#metadatavalidate"
      ),
      Completer(
        "%sql",
        "transform",
        s"""%sql name="sqltransform" outputView=outputView environments=${completionEnvironments}
        |SELECT
        |  *
        |FROM inputView""".stripMargin,
        "sql",
        "https://arc.tripl.ai/transform/#sqltransform"
      ),
      Completer(
        "%sqlvalidate",
        "validate",
        s"""%sqlvalidate name="sqlvalidate" environments=${completionEnvironments}
        |SELECT
        |  TRUE AS valid
        |  ,TO_JSON(
        |    NAMED_STRUCT(
        |      'key', 'value'
        |    )
        |  ) AS message""".stripMargin,
        "sql",
        "https://arc.tripl.ai/validate/#sqlvalidate"
      ),
      Completer(
        "%version",
        "arc",
        s"""%version""".stripMargin,
        "shell",
        ""
      ),
    )
  }

  // todo: these should come from traits in arc
  def getCompletions(
    pos: Int,
    length: Int,
    commandLineArgs: Map[String, Common.ConfigValue],
    confDatasetLabels: Boolean,
    confExtendedErrors: Boolean,
    confLeftAlign: Boolean,
    confShowLog: Boolean,
    confMonospace: Boolean,
    confNumRows: Int,
    confTruncate: Int,
    confStreaming: Boolean,
    confStreamingDuration: Int,
    confCompletionEnvironments: String,
  )(implicit spark: SparkSession, arcContext: ARCContext): Completion = {
    import spark.implicits._

    val jupyterCompletions = makeJupyterCompletions(confCompletionEnvironments)

    // progressively enable additional completions as the arcContext becomes available
    val completions = if (arcContext == null) {
      jupyterCompletions
    } else {
      // only resolve plugins once
      if (pipelinePluginCompletions.length == 0) {
        pipelinePluginCompletions = arcContext.pipelineStagePlugins.flatMap { stage =>
          stage match {
            case s: JupyterCompleter => Option(
              Completer(
                s.getClass.getSimpleName,
                s.getClass.getPackage.getName.split("\\.").last,
                s.snippet,
                s.mimetype,
                s.documentationURI.toString
              )
            )
            case _ => None
          }
        }
      }

      if (lifecyclePluginCompletions.length == 0) {
        lifecyclePluginCompletions = arcContext.lifecyclePlugins.flatMap { stage =>
          stage match {
            case s: JupyterCompleter => Option(
              Completer(
                s.getClass.getSimpleName,
                s.getClass.getPackage.getName.split("\\.").last,
                s.snippet,
                s.mimetype,
                s.documentationURI.toString
              )
            )
            case _ => None
          }
        }
      }

      val dynamicCompletions = List(
        Completer(
          "%conf",
          "arc",
          s"""%conf
          |datasetLabels=${confDatasetLabels}
          |extendedErrors=${confExtendedErrors}
          |leftAlign=${confLeftAlign}
          |showLog=${confShowLog}
          |monospace=${confMonospace}
          |numRows=${confNumRows}
          |streaming=${confStreaming}
          |streamingDuration=${confStreamingDuration}
          |truncate=${confTruncate}""".stripMargin,
          "shell",
          ""
        ),
        Completer(
          "%env",
          "arc",
          s"""%env
          |${commandLineArgs.map { case (key, configValue) => s"${key}=${if (configValue.secret) "*" * configValue.value.length else configValue.value }" }.toList.sorted.mkString("\n")}""".stripMargin,
          "shell",
          ""
        ),
      )

      // add any registered tables (see OutputTable for registration)
      if (arcContext.userData.contains(Common.TABLE_COMPLETIONS_KEY)) {
        val tableCompletions = arcContext.userData.get(Common.TABLE_COMPLETIONS_KEY).get.asInstanceOf[List[Completer]]
        (pipelinePluginCompletions ++ lifecyclePluginCompletions ++ jupyterCompletions ++ dynamicCompletions ++ tableCompletions)
      } else {
        (pipelinePluginCompletions ++ lifecyclePluginCompletions ++ jupyterCompletions ++ dynamicCompletions)
      }
    }

    val objectMapper = new ObjectMapper()
    val jsonNodeFactory = new JsonNodeFactory(true)
    val node = jsonNodeFactory.objectNode
    val jupyterTypesArray = node.putArray("_jupyter_types_experimental")
    completions.foreach { completion =>
      val completionNode = jsonNodeFactory.objectNode
      completionNode.set("text", jsonNodeFactory.textNode(completion.text))
      completionNode.set("type", jsonNodeFactory.textNode(completion.textType))
      completionNode.set("replaceText", jsonNodeFactory.textNode(completion.replaceText))
      completionNode.set("language", jsonNodeFactory.textNode(completion.language))
      completionNode.set("documentation", jsonNodeFactory.textNode(completion.documentation))
      completionNode.set("sortBy", jsonNodeFactory.textNode(s"${completion.textType}:${completion.text}"))
      jupyterTypesArray.add(completionNode)
    }

    almond.interpreter.Completion(0, length, completions.map(_.text),  RawJson(objectMapper.writeValueAsString(node).getBytes(StandardCharsets.UTF_8)))
  }

  // This function comes from the Apahce Spark org.apache.spark.util.Utils private class
  /** Load properties present in the given file. */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala
        .map { k => (k, trimExceptCRLF(properties.getProperty(k))) }
        .toMap

    } catch {
      case e: IOException =>
        throw new Exception(s"Failed when loading Spark properties from $filename", e)
    } finally {
      inReader.close()
    }
  }

  // This function comes from the Apahce Spark org.apache.spark.util.Utils private class
  /**
   * Implements the same logic as JDK `java.lang.String#trim` by removing leading and trailing
   * non-printable characters less or equal to '\u0020' (SPACE) but preserves natural line
   * delimiters according to java.util.Properties load method. The natural line delimiters are
   * removed by JDK during load. Therefore any remaining ones have been specifically provided and
   * escaped by the user, and must not be ignored
   *
   * @param str
   * @return the trimmed value of str
   */
  def trimExceptCRLF(str: String): String = {
    val nonSpaceOrNaturalLineDelimiter: Char => Boolean = { ch =>
      ch > ' ' || ch == '\r' || ch == '\n'
    }

    val firstPos = str.indexWhere(nonSpaceOrNaturalLineDelimiter)
    val lastPos = str.lastIndexWhere(nonSpaceOrNaturalLineDelimiter)
    if (firstPos >= 0 && lastPos >= 0) {
      str.substring(firstPos, lastPos + 1)
    } else {
      ""
    }
  }

  def getLogger(appender: Option[InMemoryLoggerAppender] = None)(implicit spark: SparkSession): ai.tripl.arc.util.log.logger.Logger = {
    val loader = ai.tripl.arc.util.Utils.getContextOrSparkClassLoader
    val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("breeze").setLevel(Level.ERROR)
    appender.foreach { Logger.getLogger(spark.sparkContext.applicationId).addAppender(_) }
    logger
  }
}