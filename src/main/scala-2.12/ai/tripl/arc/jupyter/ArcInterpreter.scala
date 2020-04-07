package ai.tripl.arc.jupyter

import java.util.UUID
import java.util.Properties
import java.util.ServiceLoader
import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.Try
import util.control.Breaks._

import almond.interpreter.{Completion, ExecuteResult, Inspection, Interpreter}
import almond.interpreter.api.{DisplayData, OutputHandler}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import almond.interpreter.input.InputManager
import almond.protocol.internal.ExtraCodecs._
import almond.protocol.KernelInfo
import argonaut._
import argonaut.Argonaut._

import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import com.typesafe.config._

import org.opencypher.morpheus.api.MorpheusSession

import ai.tripl.arc.ARC
import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.config.ArcPipeline
import ai.tripl.arc.plugins._
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.log.LoggerFactory

import java.lang.management.ManagementFactory

case class ConfigValue (
  secret: Boolean,
  value: String
)

final class ArcInterpreter extends Interpreter {

  implicit var spark: SparkSession = _

  val secretPattern = """"(token|signature|accessKey|secret|secretAccessKey)":[\s]*".*"""".r

  var confMaster: String = "local[*]"
  var confNumRows = 20
  var confTruncate = 50
  var confCommandLineArgs: Map[String, ConfigValue] = Map.empty
  var confStreaming = false
  var confStreamingDuration = 10
  var confStreamingFrequency = 1000
  var confMonospace = false
  var udfsRegistered = false

  var isJupyterLab: Option[Boolean] = None

  // resolution is slow so dont keep repeating
  var memoizedPipelineStagePlugins: Option[List[ai.tripl.arc.plugins.PipelineStagePlugin]] = None
  var memoizedUDFPlugins: Option[List[ai.tripl.arc.plugins.UDFPlugin]] = None
  var memoizedDynamicConfigPlugins: Option[List[ai.tripl.arc.plugins.DynamicConfigurationPlugin]] = None

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
      s"""Arc kernel Java ${sys.props.getOrElse("java.version", "[unknown]")}""".stripMargin
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
          .filter{ case (key, value) => key.startsWith("conf_") }
          .foldLeft(sessionBuilder: SparkSession.Builder){ case (sessionBuilder, (key: String, value: String)) => {
            sessionBuilder.config(key.replaceFirst("conf_","").replaceAll("_", "."), value)
          }}

        val session = sessionBuilder.getOrCreate()
        spark = session

        implicit val logger = LoggerFactory.getLogger("")
        val loader = ai.tripl.arc.util.Utils.getContextOrSparkClassLoader

        import session.implicits._

        // detect jupyterlab
        val jupyterLab = isJupyterLab.getOrElse(
          scala.util.Properties.envOrNone("JUPYTER_ENABLE_LAB") match {
            case Some(j) if (j == "yes") => true
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
            val commandArgs = parseArgs(lines(0))
            val name = commandArgs.get("name") match {
              case Some(name) => name
              case None => ""
            }
            val description = commandArgs.get("description") match {
              case Some(description) => description
              case None => ""
            }
            val sqlParams = commandArgs.get("sqlParams") match {
              case Some(sqlParams) => {
                parseArgs(sqlParams.replace(",", " ")).map{
                  case (k, v) => {
                    if (v.trim().startsWith("${")) {
                      s""""${k}": ${v}"""
                    } else {
                      s""""${k}": "${v}""""
                    }
                  }
                }.mkString(",")
              }
              case None => ""
            }      

            ("arc", parseArgs(lines(0)),
              if (lines(0).startsWith("%sqlvalidate")) {
                s"""{
                |  "type": "SQLValidate",
                |  "name": "${name}",
                |  "description": "${description}",
                |  "environments": [],
                |  "sql": \"\"\"${lines.drop(1).mkString("\n")}\"\"\",
                |  "sqlParams": {${sqlParams}},
                |  ${commandArgs.filterKeys{ !List("name", "description", "sqlParams", "environments", "numRows", "truncate", "persist", "monospace", "streamingDuration").contains(_) }.map{ case (k, v) => s""""${k}": "${v}""""}.mkString(",")}
                |}""".stripMargin
              } else {
                s"""{
                |  "type": "SQLTransform",
                |  "name": "${name}",
                |  "description": "${description}",
                |  "environments": [],
                |  "sql": \"\"\"${lines.drop(1).mkString("\n")}\"\"\",
                |  "outputView": "${commandArgs.getOrElse("outputView", randStr(32))}",
                |  "persist": ${commandArgs.getOrElse("persist", "false")},
                |  "sqlParams": {${sqlParams}}
                |  ${commandArgs.filterKeys{ !List("name", "description", "sqlParams", "environments", "outputView", "numRows", "truncate", "persist", "monospace", "streamingDuration").contains(_) }.map{ case (k, v) => s""""${k}": "${v}""""}.mkString(",")}
                |}""".stripMargin
              }
            )
          }
          case x: String if (x.startsWith("%cypher")) => {
            ("cypher", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x: String if (x.startsWith("%configplugin")) => {
            ("configplugin", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x: String if (x.startsWith("%lifecycleplugin")) => {
            ("lifecycleplugin", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
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
          case x: String if (x.startsWith("%secret")) => {
            ("secret", parseArgs(lines.mkString(" ")), lines.drop(1).mkString("\n"))
          }
          case x: String if (x.startsWith("%conf")) => {
            ("conf", parseArgs(lines.mkString(" ")), "")
          }
          case x: String if (x.startsWith("%version")) => {
            ("version", parseArgs(lines(0)), "")
          }
          case x: String if (x.startsWith("%help")) => {
            ("help", parseArgs(""), "")
          }
          case _ => ("arc", collection.mutable.Map[String, String](), code.trim)
        }

        val numRows = Try(commandArgs.get("numRows").get.toInt).getOrElse(confNumRows)
        val truncate = Try(commandArgs.get("truncate").get.toInt).getOrElse(confTruncate)
        val streamingDuration = Try(commandArgs.get("streamingDuration").get.toInt).getOrElse(confStreamingDuration)
        val persist = Try(commandArgs.get("persist").get.toBoolean).getOrElse(false)
        val monospace = Try(commandArgs.get("monospace").get.toBoolean).getOrElse(confMonospace)

        // store previous values so that the ServiceLoader resolution is not called each run
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
        val dynamicConfigsPlugins = memoizedDynamicConfigPlugins match {
          case Some(dynamicConfigsPlugins) => dynamicConfigsPlugins
          case None => {
            memoizedDynamicConfigPlugins = Option(ServiceLoader.load(classOf[DynamicConfigurationPlugin], loader).iterator().asScala.toList)
            memoizedDynamicConfigPlugins.get
          }
        }

        implicit val arcContext = ARCContext(
          jobId=None,
          jobName=None,
          environment=None,
          environmentId=None,
          configUri=None,
          isStreaming=confStreaming,
          ignoreEnvironments=true,
          commandLineArguments=confCommandLineArgs.map { case (key, config) => (key, config.value) },
          storageLevel=StorageLevel.MEMORY_AND_DISK_SER,
          immutableViews=false,
          dynamicConfigurationPlugins=dynamicConfigsPlugins,
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
              case "arc" | "summary" | "cypher" => {
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
            // ensure that the input text does not have secrets
            secretPattern.findFirstIn(command) match {
              case Some(_) => ExecuteResult.Error("Secret found in input. Use %secret to define to prevent accidental leaks.")
              case None => {
                val pipelineEither = ArcPipeline.parseConfig(Left(s"""{"stages": [${command}]}"""), arcContext)

                pipelineEither match {
                  case Left(errors) => ExecuteResult.Error(ai.tripl.arc.config.Error.pipelineSimpleErrorMsg(errors, false))
                  case Right((pipeline, _)) => {
                    pipeline.stages.length match {
                      case 0 => {
                        ExecuteResult.Error("No stages found.")
                      }
                      case _ => {
                        ARC.run(pipeline) match {
                          case Some(df) => {
                            val result = renderResult(outputHandler, df, numRows, truncate, monospace, streamingDuration)
                            memoizedUserData = arcContext.userData
                            result
                          }
                          case None => {
                            ExecuteResult.Success(DisplayData.text("No result."))
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
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
            ExecuteResult.Success(
              DisplayData.html(renderHTML(df, numRows, truncate, monospace))
            )
          }
          case "configplugin" => {
            val config = ConfigFactory.parseString(s"""{"plugins": {"config": [${command}]}}""", ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
            val dynamicConfigsOrErrors = ai.tripl.arc.config.Plugins.resolveConfigPlugins(config, "plugins.config", arcContext.dynamicConfigurationPlugins)(spark, logger, arcContext)
            dynamicConfigsOrErrors match {
                case Left(errors) => ExecuteResult.Error(ai.tripl.arc.config.Error.pipelineSimpleErrorMsg(errors, false))
              case Right(dynamicConfigs) => {
                val dynamicConfigsConf = dynamicConfigs.reduceRight[Config]{ case (c1, c2) => c1.withFallback(c2) }
                val entryMap = dynamicConfigsConf.entrySet.asScala.map { entry =>
                  entry.getKey -> ConfigValue(false, entry.getValue.unwrapped.toString)
                }.toMap
                confCommandLineArgs = confCommandLineArgs ++ entryMap
              }
              ExecuteResult.Success(DisplayData.text(confCommandLineArgs.map { case (key, configValue) => s"${key}: ${if (configValue.secret) "*" * configValue.value.length else configValue.value }" }.toList.sorted.mkString("\n")))
            }
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
            ExecuteResult.Success(
              DisplayData.html(renderHTML(df, numRows, truncate, monospace))
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
            ExecuteResult.Success(
              DisplayData.html(renderHTML(df, numRows, truncate, monospace))
            )
          }
          case "env" => {
            if (!commandArgs.isEmpty) {
              confCommandLineArgs = commandArgs.map { case (key, value) => key -> ConfigValue(false, value) }.toMap
            }
            ExecuteResult.Success(DisplayData.text(confCommandLineArgs.map { case (key, configValue) => s"${key}: ${if (configValue.secret) "*" * configValue.value.length else configValue.value }" }.toList.sorted.mkString("\n")))
          }
          case "secret" => {
            val secrets = collection.mutable.Map[String, ConfigValue]()
            command.split("\n").map(_.trim).foreach { key =>
              val value = inputManager match {
                case Some(im) => Await.result(im.password(key), Duration.Inf)
                case None => ""
              }
              secrets += (key -> ConfigValue(true, value))
            }

            confCommandLineArgs = confCommandLineArgs ++ secrets
            ExecuteResult.Success(DisplayData.text(confCommandLineArgs.map { case (key, configValue) => s"${key}: ${if (configValue.secret) "*" * configValue.value.length else configValue.value }" }.toList.sorted.mkString("\n")))
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
            commandArgs.get("monospace") match {
              case Some(monospace) => {
                try {
                  val monospaceValue = monospace.toBoolean
                  confMonospace = monospaceValue
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
            |memory: ${runtimeMemorySize}B
            |monospace: ${confMonospace}
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

      val error = executeResult match {
        case _: ExecuteResult.Error => true
        case _ => false
      }

      removeListener(spark, executionListener, error)(outputHandler)
      executeResult
    } catch {
      case e: Exception => {
        removeListener(spark, executionListener, true)(outputHandler)
        ExecuteResult.Error(e.getMessage)
      }
    }
  }

  def removeListener(spark: SparkSession, listener: Option[ProgressSparkListener], error: Boolean)(implicit outputHandler: Option[OutputHandler]) {
    (listener, outputHandler) match {
      case (Some(listener), Some(outputHandler)) => {
        listener.update(error, true)(outputHandler)
        spark.sparkContext.removeSparkListener(listener)
      }
      case _ =>
    }
  }

  def renderResult(outputHandler: Option[OutputHandler], df: DataFrame, numRows: Int, truncate: Int, monospace: Boolean, streamingDuration: Int) = {
    if (!df.isStreaming) {
      ExecuteResult.Success(
        DisplayData.html(renderHTML(df, numRows, truncate, monospace))
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
                  renderHTML(df, numRows, truncate, monospace),
                  outputElementHandle
                )
                initial = false
              } else {
                outputHandler.updateHtml(
                  renderHTML(df, numRows, truncate, monospace),
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
            DisplayData.html(renderHTML(spark.table(queryName), numRows, truncate, monospace))
          )
        }
        case None => ExecuteResult.Error("No result.")
      }
    }
  }

  def currentLine(): Int =
    count

  def renderHTML(df: DataFrame, numRows: Int, truncate: Int, monospace: Boolean): String = {
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

    val tableClass = if (monospace) """class="monospace"""" else ""

    s"""<table ${tableClass}><tr>${header.map(h => s"<th>${escape(h)}</th>").mkString}</tr>${rows.map { row => s"<tr>${row.map { cell => s"<td>${escape(cell)}</td>" }.mkString}</tr>"}.mkString}</table>"""
  }

  def parseArgs(input: String): collection.mutable.Map[String, String] = {
    val args = collection.mutable.Map[String, String]()
    val (vals, opts) = input.split("\\s(?=([^\"']*\"[^\"]*\")*[^\"']*$)").partition {
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