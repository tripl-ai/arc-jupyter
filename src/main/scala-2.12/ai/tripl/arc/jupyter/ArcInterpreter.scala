package ai.tripl.arc.jupyter

import java.util.Properties
import java.util.ServiceLoader
import scala.collection.JavaConverters._
import scala.util.Random

import almond.interpreter.{Completion, ExecuteResult, Inspection, Interpreter}
import almond.interpreter.api.{DisplayData, OutputHandler}
import almond.interpreter.input.InputManager
import almond.protocol.internal.ExtraCodecs._
import almond.protocol.KernelInfo
import argonaut._
import argonaut.Argonaut._

import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel

import org.opencypher.morpheus.api.MorpheusSession

import ai.tripl.arc.ARC
import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.config.ArcPipeline
import ai.tripl.arc.plugins._
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.log.LoggerFactory

import java.lang.management.ManagementFactory

final class ArcInterpreter extends Interpreter {

  implicit var spark: SparkSession = _

  var confMaster: String = "local[*]"
  var confNumRows = 20
  var confTruncate = 50
  var confCommandLineArgs: Map[String, String] = Map.empty
  var confStreaming = false
  var confStreamingDuration = 10
  var confStreamingFrequency = 1000
  var udfsRegistered = false

  var isJupyterLab: Option[Boolean] = None

  // resolution is slow so dont keep repeating
  var memoizedPipelineStagePlugins: Option[List[ai.tripl.arc.plugins.PipelineStagePlugin]] = None
  var memoizedUDFPlugins: Option[List[ai.tripl.arc.plugins.UDFPlugin]] = None

  // cache userData so state can be preserved between executions
  var memoizedUserData: collection.mutable.Map[String, Object] = collection.mutable.Map.empty

  def kernelInfo(): KernelInfo =
    KernelInfo(
      "arc",
      ai.tripl.arc.jupyter.BuildInfo.version,
      KernelInfo.LanguageInfo(
        "arc",
        ai.tripl.arc.jupyter.BuildInfo.version,
        "text/arc",
        "arc",
        "text" // ???
      ),
      s"""Arc kernel |Java ${sys.props.getOrElse("java.version", "[unknown]")}""".stripMargin
    )

  @volatile private var count = 0

  val alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  val size = alpha.size
  def randStr(n:Int) = (1 to n).map(x => alpha(Random.nextInt.abs % size)).mkString

  def execute(
    code: String,
    storeHistory: Boolean,
    inputManager: Option[InputManager],
    outputHandler: Option[OutputHandler]
  ): ExecuteResult = {
    val listenerElementHandle = randStr(32)
    var executionListener: Option[ProgressSparkListener] = None

    try {
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("breeze").setLevel(Level.ERROR)

      // the memory available to the container (i.e. the docker memory limit)
      val physicalMemorySize = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean].getTotalPhysicalMemorySize
      // the JVM requested memory (-Xmx)
      val runtimeMemorySize = Runtime.getRuntime.maxMemory
      val executeResult = if (runtimeMemorySize > physicalMemorySize) {
        return ExecuteResult.Error(s"Cannot execute as requested JVM memory (-Xmx${runtimeMemorySize}B) exceeds available Docker memory (${physicalMemorySize}B) limit.\nEither decrease the requested JVM memory or increase the Docker memory limit.")
      } else {

        val sessionBuilder = SparkSession
          .builder()
          .master(confMaster)
          .appName("arc-jupyter")
          .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
          .config("spark.rdd.compress", true)
          .config("spark.sql.cbo.enabled", true)
          .config("spark.driver.maxResultSize", s"${(runtimeMemorySize * 0.8).toLong}b")

        // add any spark overrides
        System.getenv.asScala
          .filter{ case (key, value) => key.startsWith("spark_") }
          .foldLeft(sessionBuilder: SparkSession.Builder){ case (sessionBuilder, (key: String, value: String)) => {
          sessionBuilder.config(key.replaceAll("_", ".").toLowerCase, value)
        }}

        val session = sessionBuilder.getOrCreate()
        spark = session

        implicit val logger = LoggerFactory.getLogger("")
        val loader = ai.tripl.arc.util.Utils.getContextOrSparkClassLoader

        import session.implicits._

        // detect jupyterlab
        val jupyterLab = isJupyterLab.getOrElse(
          scala.util.Properties.envOrNone("JUPYTER_ENABLE_LAB") match {
            case Some(j) => if (j == "yes") true else false
            case None => false
          }
        )

        // parse input
        val lines = code.trim.split("\n")
        val (interpreter, commandArgs, command) = lines(0) match {
          case x: String if (x.startsWith("%arc")) => {
            ("arc", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x: String if (x.startsWith("%sql")) => {
            ("sql", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x: String if (x.startsWith("%cypher")) => {
            ("cypher", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x: String if (x.startsWith("%schema")) => {
            ("schema", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x: String if (x.startsWith("%printschema")) => {
            ("printschema", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x: String if (x.startsWith("%metadata")) => {
            ("metadata", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x: String if (x.startsWith("%printmetadata")) => {
            ("printmetadata", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x: String if (x.startsWith("%summary")) => {
            ("summary", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x: String if (x.startsWith("%env")) => {
            ("env", parseArgs(lines.mkString(" ")), "")
          }
          case x: String if (x.startsWith("%conf")) => {
            ("conf", parseArgs(lines.mkString(" ")), "")
          }
          case x: String if (x.startsWith("%version")) => {
            ("version", parseArgs(lines(0)), "")
          }
          case _ => ("arc", collection.mutable.Map[String, String](), code.trim)
        }

        val numRows = commandArgs.get("numRows") match {
          case Some(numRows) => numRows.toInt
          case None => confNumRows
        }
        val truncate = commandArgs.get("truncate") match {
          case Some(truncate) => truncate.toInt
          case None => confTruncate
        }
        val streamingDuration = commandArgs.get("streamingDuration") match {
          case Some(streamingDuration) => streamingDuration.toInt
          case None => confStreamingDuration
        }
        val persist = commandArgs.get("persist") match {
          case Some(persist) => persist.toBoolean
          case None => false
        }

        val pipelineStagePlugins = memoizedPipelineStagePlugins match {
          case Some(pipelineStagePlugins) => pipelineStagePlugins
          case None => {
            memoizedPipelineStagePlugins = Option(ServiceLoader.load(classOf[PipelineStagePlugin], loader).iterator().asScala.toList)
            memoizedPipelineStagePlugins.get
          }
        }
        val udfPlugins = memoizedUDFPlugins match {
          case Some(udfPlugins) => udfPlugins
          case None => {
            memoizedUDFPlugins = Option(ServiceLoader.load(classOf[UDFPlugin], loader).iterator().asScala.toList)
            memoizedUDFPlugins.get
          }
        }

        // store previous value so that the ServiceLoader resolution is not called each run
        implicit val arcContext = ARCContext(
          jobId=None,
          jobName=None,
          environment=None,
          environmentId=None,
          configUri=None,
          isStreaming=confStreaming,
          ignoreEnvironments=true,
          commandLineArguments=confCommandLineArgs,
          storageLevel=StorageLevel.MEMORY_AND_DISK_SER,
          immutableViews=false,
          dynamicConfigurationPlugins=Nil,
          lifecyclePlugins=Nil,
          activeLifecyclePlugins=Nil,
          pipelineStagePlugins=pipelineStagePlugins,
          udfPlugins=udfPlugins,
          userData=memoizedUserData
        )

        // register udfs once
        if (!udfsRegistered) {
          ai.tripl.arc.udf.UDF.registerUDFs()(spark, logger, arcContext)
          udfsRegistered = true
        }

        outputHandler match {
          case Some(outputHandler) => {
            interpreter match {
              case "arc" | "sql" | "summary" | "cypher" => {
                val listener = new ProgressSparkListener(listenerElementHandle, jupyterLab)(outputHandler, logger)
                listener.init()(outputHandler)
                spark.sparkContext.addSparkListener(listener)
                executionListener = Option(listener)
              }
              case _ =>
            }
          }
          case None => None
        }

        interpreter match {
          case "arc" => {

            val pipelineEither = ArcPipeline.parseConfig(Left(s"""{"stages": [${command}]}"""), arcContext)

            pipelineEither match {
              case Left(errors) => {
                ExecuteResult.Error(ai.tripl.arc.config.Error.pipelineSimpleErrorMsg(errors))
              }
              case Right((pipeline, _)) => {
                pipeline.stages.length match {
                  case 0 => {
                    ExecuteResult.Error("No stages found.")
                  }
                  case _ => {
                    ARC.run(pipeline) match {
                      case Some(df) => {
                        val result = renderResult(outputHandler, df, numRows, truncate, streamingDuration)
                        memoizedUserData = arcContext.userData
                        result
                      }
                      case None => {
                        ExecuteResult.Error("No result.")
                      }
                    }
                  }
                }
              }
            }
          }
          case "sql" => {
            val df = spark.sql(SQLUtils.injectParameters(command, arcContext.commandLineArguments, true))
            commandArgs.get("outputView") match {
              case Some(ov) => df.createOrReplaceTempView(ov)
              case None =>
            }
            if (persist) df.persist(StorageLevel.MEMORY_AND_DISK_SER)
            renderResult(outputHandler, df, numRows, truncate, confStreamingDuration)
          }
          case "cypher" => {
            // the morpheus session must be created by the GraphTransform stage
            val morpheusSession = arcContext.userData.get("morpheusSession") match {
              case Some(morpheusSession: MorpheusSession) => morpheusSession
              case _ => throw new Exception(s"CypherTransform executes an existing graph created with GraphTransform but no session exists to execute CypherTransform against.")
            }
            val df = morpheusSession.cypher(SQLUtils.injectParameters(command, arcContext.commandLineArguments, true)).records.table.df
            commandArgs.get("outputView") match {
              case Some(ov) => df.createOrReplaceTempView(ov)
              case None =>
            }
            if (persist) df.persist(StorageLevel.MEMORY_AND_DISK_SER)
            val (html, _) = renderHTML(df, numRows, truncate)
            ExecuteResult.Success(
              DisplayData.html(html)
            )
          }
          case "schema" => {
            ExecuteResult.Success(
              DisplayData.text(spark.table(command).schema.prettyJson)
            )
          }
          case "printschema" => {
            ExecuteResult.Success(
              DisplayData.text(spark.table(command).schema.treeString)
            )
          }
          case "metadata" => {
            val df = Common.createPrettyMetadataDataframe(spark.table(command))
            commandArgs.get("outputView") match {
              case Some(ov) => df.createOrReplaceTempView(ov)
              case None =>
            }
            if (persist) df.persist(StorageLevel.MEMORY_AND_DISK_SER)
            val (html, _) = renderHTML(df, numRows, truncate)
            ExecuteResult.Success(
              DisplayData.html(html)
            )
          }
          case "printmetadata" => {
            ExecuteResult.Success(
              DisplayData.text(MetadataUtils.makeMetadataFromDataframe(spark.table(command)))
            )
          }
          case "summary" => {
            val df = spark.table(command).summary()
            commandArgs.get("outputView") match {
              case Some(ov) => df.createOrReplaceTempView(ov)
              case None =>
            }
            if (persist) df.persist(StorageLevel.MEMORY_AND_DISK_SER)
            val (html, _) = renderHTML(df, numRows, truncate)
            ExecuteResult.Success(
              DisplayData.html(html)
            )
          }
          case "env" => {
            confCommandLineArgs = commandArgs.toMap
            ExecuteResult.Success(DisplayData.text(confCommandLineArgs.map { case (key, value) => s"${key}: ${value}" }.mkString("\n")))
          }
          case "conf" => {
            commandArgs.get("master") match {
              case Some(master) => {
                confMaster = master
                spark.stop
              }
              case None =>
            }
            if (confNumRows != numRows) confNumRows = numRows
            if (confTruncate != truncate) confTruncate = truncate
            commandArgs.get("streaming") match {
              case Some(streaming) => {
                try {
                  val streamingValue = streaming.toBoolean
                  confStreaming = streamingValue
                } catch {
                  case e: Exception =>
                }
              }
              case None =>
            }
            commandArgs.get("streamingDuration") match {
              case Some(streamingDuration) => {
                try {
                  val streamingDurationValue = streamingDuration.toInt
                  confStreamingDuration = streamingDurationValue
                } catch {
                  case e: Exception =>
                }
              }
              case None =>
            }
            val text = s"""
            |master: ${confMaster}
            |numRows: ${confNumRows}
            |truncate: ${confTruncate}
            |streaming: ${confStreaming}
            |streamingDuration: ${confStreamingDuration}
            """.stripMargin
            ExecuteResult.Success(
              DisplayData.text(text)
            )
          }
          case "version" => {
            ExecuteResult.Success(
              DisplayData.text(Common.GetVersion())
            )
          }
          case "help" => {
            ExecuteResult.Success(
              DisplayData.text(Common.GetHelp)
            )
          }
        }
      }

      removeListener(spark, executionListener, false)(outputHandler)
      executeResult
    } catch {
      case e: Exception => {
        removeListener(spark, executionListener, true)(outputHandler)
        ExecuteResult.Error(e.getMessage)
      }
    }
  }

  def removeListener(spark: SparkSession, listener: Option[ProgressSparkListener], error: Boolean)(implicit outputHandler: Option[OutputHandler]) {
    listener match {
      case Some(listener) => {
        if (error && outputHandler.nonEmpty) {
          listener.update(true, true)(outputHandler.get)
        } else {
          listener.update(false, true)(outputHandler.get)
        }
        spark.sparkContext.removeSparkListener(listener)
      }
      case None =>
    }
  }

  def renderResult(outputHandler: Option[OutputHandler], df: DataFrame, numRows: Int, truncate: Int, streamingDuration: Int) = {
    if (!df.isStreaming) {
      val (html, _) = renderHTML(df, numRows, truncate)
      ExecuteResult.Success(
        DisplayData.html(html)
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
          val startTime = System.currentTimeMillis
          var initial = true
          var length = 0
          while (System.currentTimeMillis <= startTime + (streamingDuration * 1000) && length < numRows) {
            if (initial) {
              outputHandler.html("", outputElementHandle)
              initial = false
            } else {
              val (html, rows) = renderHTML(spark.sql(s"SELECT * FROM ${queryName}"), numRows, truncate)
              outputHandler.updateHtml(
                html,
                outputElementHandle
              )
              length = rows
            }
            Thread.sleep(confStreamingFrequency)
          }

          // stop stream and display final result
          writeStream.stop
          outputHandler.html("", outputElementHandle)
          val (html, _) = renderHTML(spark.sql(s"SELECT * FROM ${queryName}"), numRows, truncate)
          ExecuteResult.Success(
            DisplayData.html(html)
          )
        }
        case None => ExecuteResult.Error("No result.")
      }
    }
  }

  def currentLine(): Int =
    count

  def renderHTML(df: DataFrame, numRows: Int, truncate: Int): (String, Int) = {
    import xml.Utility.escape

    val data = df.take(numRows)
    val header = df.schema.fieldNames.toSeq

    val _rows: Seq[Seq[String]] = data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
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

    (s"""
    <table>
        <tr>
          ${header.map(h => s"<th>${escape(h)}</th>").mkString}
        </tr>
        ${_rows.map { r =>
          s"<tr>${r.map { c => s"<td>${escape(c)}</td>" }.mkString}</tr>"
        }.mkString}
    </table>
    """,
    data.length)
  }

  def parseArgs(input: String): collection.mutable.Map[String, String] = {
    val args = collection.mutable.Map[String, String]()
    val (vals, opts) = input.split(" ").partition {
      _.startsWith("%")
    }
    opts.map { x =>
      // regex split on only single = signs not at start or end of line
      val pair = x.split("=(?!=)(?!$)", 2)
      if (pair.length == 2) {
        args += (pair(0) -> pair(1))
      }
    }

    args
  }

}