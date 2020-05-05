package ai.tripl.arc.jupyter

import java.lang.management.ManagementFactory
import java.net.URI
import java.security.SecureRandom
import java.sql.Timestamp
import java.time.Instant
import java.util.Properties
import java.util.ServiceLoader
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.Properties._
import util.control.Breaks._

import almond.interpreter.{Completion, ExecuteResult, Inspection, Interpreter}
import almond.interpreter.api.{DisplayData, OutputHandler}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import almond.interpreter.input.InputManager
import almond.protocol.KernelInfo

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import com.typesafe.config._

import org.opencypher.morpheus.api.MorpheusSession

import ai.tripl.arc.ARC
import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.config.ArcPipeline
import ai.tripl.arc.plugins._
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.log.LoggerFactory
import ai.tripl.arc.util.SerializableConfiguration


case class ConfigValue (
  secret: Boolean,
  value: String
)

case class FileDisplay(
  path: String,
  name: String,
  modificationTime: Timestamp,
  size: String,
  bytes: Long
)

final class ArcInterpreter extends Interpreter {

  implicit var spark: SparkSession = _

  val authenticateSecret = Common.randStr(64)

  val secretPattern = """"(token|signature|accessKey|secret|secretAccessKey)":[\s]*".*"""".r

  var confCommandLineArgs: Map[String, ConfigValue] = Map.empty
  var confMaster = envOrNone("CONF_MASTER").getOrElse("local[*]")
  var confNumRows = Try(envOrNone("CONF_NUM_ROWS").get.toInt).getOrElse(20)
  var confTruncate = Try(envOrNone("CONF_TRUNCATE").get.toInt).getOrElse(50)
  var confStreaming = false
  var confStreamingDuration = Try(envOrNone("CONF_STREAMING_DURATION").get.toInt).getOrElse(10)
  var confStreamingFrequency = Try(envOrNone("CONF_STREAMING_FREQUENCY").get.toInt).getOrElse(1000)
  var confMonospace = Try(envOrNone("CONF_DISPLAY_MONOSPACE").get.toBoolean).getOrElse(false)
  var confLeftAlign = Try(envOrNone("CONF_DISPLAY_LEFT_ALIGN").get.toBoolean).getOrElse(false)
  var confDatasetLabels = Try(envOrNone("CONF_DISPLAY_DATASET_LABELS").get.toBoolean).getOrElse(false)
  var udfsRegistered = false

  var isJupyterLab: Option[Boolean] = None

  // resolution is slow so dont keep repeating
  var memoizedPipelineStagePlugins: Option[List[ai.tripl.arc.plugins.PipelineStagePlugin]] = None
  var memoizedUDFPlugins: Option[List[ai.tripl.arc.plugins.UDFPlugin]] = None
  var memoizedDynamicConfigPlugins: Option[List[ai.tripl.arc.plugins.DynamicConfigurationPlugin]] = None
  var memoizedLifecyclePlugins: Option[List[ai.tripl.arc.plugins.LifecyclePlugin]] = None

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

  def execute(
    code: String,
    storeHistory: Boolean,
    inputManager: Option[InputManager],
    outputHandler: Option[OutputHandler]
  ): ExecuteResult = {
    val listenerElementHandle = Common.randStr(32)
    var executionListener: Option[ProgressSparkListener] = None

    try {
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("breeze").setLevel(Level.ERROR)

      // the memory available to the container (i.e. the docker memory limit)
      val physicalMemory = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean].getTotalPhysicalMemorySize
      // the JVM requested memory (-Xmx)
      val runtimeMemory = Runtime.getRuntime.maxMemory
      val executeResult = if (runtimeMemory > physicalMemory) {
        return ExecuteResult.Error(s"Cannot execute as requested JVM memory (-Xmx${FileUtils.byteCountToDisplaySize(runtimeMemory)}B) exceeds available system memory (${FileUtils.byteCountToDisplaySize(physicalMemory)}B) limit.\nEither decrease the requested JVM memory or, if running in Docker, increase the Docker memory limit.")
      } else {

        val firstRun = SparkSession.getActiveSession.isEmpty

        val sessionBuilder = SparkSession
          .builder()
          .master(confMaster)
          .appName("arc-jupyter")
          .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
          .config("spark.rdd.compress", true)
          .config("spark.sql.cbo.enabled", true)
          .config("spark.authenticate", true)
          .config("spark.authenticate.secret", authenticateSecret)
          .config("spark.io.encryption.enable", true)
          .config("spark.network.crypto.enabled", true)
          .config("spark.driver.maxResultSize", s"${(runtimeMemory * 0.8).toLong}B")

        // add any spark overrides
        System.getenv.asScala
          .filter { case (key, _) => key.startsWith("conf_spark") }
          // apply hadoop options after spark session creation
          .filter { case (key, _) => !key.startsWith("conf_spark_hadoop") }
          // you cannot override these settings for security
          .filter { case (key, _) => !Seq("conf_spark_authenticate", "conf_spark_authenticate_secret", "conf_spark_io_encryption_enable", "conf_spark_network_crypto_enabled").contains(key) }
          .foldLeft(sessionBuilder: SparkSession.Builder){ case (sessionBuilder, (key: String, value: String)) => {
            sessionBuilder.config(key.replaceFirst("conf_","").replaceAll("_", "."), value)
          }}

        val session = sessionBuilder.getOrCreate()
        spark = session

        // add any hadoop overrides
        System.getenv.asScala
          .filter { case (key, _) => key.startsWith("conf_spark_hadoop") }
          .foreach { case (key, value) => {
            spark.sparkContext.hadoopConfiguration.set(key.replaceFirst("conf_spark_hadoop_","").replaceAll("_", "."), value)
          }}

        val loader = ai.tripl.arc.util.Utils.getContextOrSparkClassLoader

        implicit val logger = LoggerFactory.getLogger("arc-jupyter")

        if (firstRun) {
          val sparkConf = new java.util.HashMap[String, String]()
          spark.sparkContext.getConf.getAll.filter{ case (k, _) => !Seq("spark.authenticate.secret").contains(k) }.foreach{ case (k, v) => sparkConf.put(k, v) }
          logger.info()
            .field("config", sparkConf)
            .field("sparkVersion", spark.version)
            .field("arcVersion", ai.tripl.arc.util.Utils.getFrameworkVersion)
            .field("arcJupyterVersion", ai.tripl.arc.jupyter.BuildInfo.version)
            .field("hadoopVersion", org.apache.hadoop.util.VersionInfo.getVersion)
            .field("scalaVersion", scala.util.Properties.versionNumberString)
            .field("javaVersion", System.getProperty("java.runtime.version"))
            .field("runtimeMemory", s"${runtimeMemory}B")
            .field("physicalMemory", s"${physicalMemory}B")
            .log()

          // only set default aws provider override if not provided
          if (Option(spark.sparkContext.hadoopConfiguration.get("fs.s3a.aws.credentials.provider")).isEmpty) {
            spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", ai.tripl.arc.util.CloudUtils.defaultAWSProvidersOverride)
          }
        }

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
          case x if (x.startsWith("%arc")) => {
            ("arc", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x if (x.startsWith("%sql") || x.startsWith("%log")) => {
            val commandArgs = parseArgs(lines(0))
            val name = commandArgs.get("name") match {
              case Some(name) => name
              case None => ""
            }
            val description = commandArgs.get("description") match {
              case Some(description) => description
              case None => ""
            }
            val envParams = confCommandLineArgs.map { case (key, config) => (key, config.value) }
            val sqlParams = commandArgs.get("sqlParams") match {
              case Some(sqlParams) => parseArgs(Common.injectParameters(sqlParams.replace(",", " "), envParams))
              case None => Map[String, String]()
            }
            val params = envParams ++ sqlParams
            val stmt = SQLUtils.injectParameters(lines.drop(1).mkString("\n"), params, true)
            ("arc", parseArgs(lines(0)),
              lines(0) match {
                case x if x.startsWith("%sqlvalidate") =>
                  s"""{
                    |  "type": "SQLValidate",
                    |  "name": "${name}",
                    |  "description": "${description}",
                    |  "environments": [],
                    |  "sql": \"\"\"${stmt}\"\"\",
                    |  ${commandArgs.filterKeys{ !List("name", "description", "sqlParams", "environments", "numRows", "truncate", "persist", "monospace", "leftAlign", "datasetLabels", "streamingDuration").contains(_) }.map{ case (k, v) => s""""${k}": "${v}""""}.mkString(",")}
                    |}""".stripMargin
                case x if x.startsWith("%sql") =>
                  s"""{
                    |  "type": "SQLTransform",
                    |  "name": "${name}",
                    |  "description": "${description}",
                    |  "environments": [],
                    |  "sql": \"\"\"${stmt}\"\"\",
                    |  "outputView": "${commandArgs.getOrElse("outputView", Common.randStr(32))}",
                    |  "persist": ${commandArgs.getOrElse("persist", "false")},
                    |  ${commandArgs.filterKeys{ !List("name", "description", "sqlParams", "environments", "outputView", "numRows", "truncate", "persist", "monospace", "leftAlign", "datasetLabels", "streamingDuration").contains(_) }.map{ case (k, v) => s""""${k}": "${v}""""}.mkString(",")}
                    |}""".stripMargin
                case x if x.startsWith("%log") =>
                  s"""{
                    |  "type": "LogExecute",
                    |  "name": "${name}",
                    |  "description": "${description}",
                    |  "environments": [],
                    |  "sql": \"\"\"${stmt}\"\"\",
                    |  ${commandArgs.filterKeys{ !List("name", "description", "sqlParams", "environments", "numRows", "truncate", "persist", "monospace", "leftAlign", "datasetLabels", "streamingDuration").contains(_) }.map{ case (k, v) => s""""${k}": "${v}""""}.mkString(",")}
                    |}""".stripMargin
                case _ => ""
              }
            )
          }
          case x if (x.startsWith("%cypher")) => {
            ("cypher", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x if (x.startsWith("%configplugin")) => {
            ("configplugin", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x if (x.startsWith("%lifecycleplugin")) => {
            ("lifecycleplugin", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x if (x.startsWith("%schema")) => {
            ("schema", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x if (x.startsWith("%printschema")) => {
            ("printschema", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x if (x.startsWith("%metadata")) => {
            ("metadata", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x if (x.startsWith("%printmetadata")) => {
            ("printmetadata", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x if (x.startsWith("%summary")) => {
            ("summary", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }
          case x if (x.startsWith("%list")) => {
            ("list", parseArgs(lines(0)), lines.drop(1).mkString("\n"))
          }          
          case x if (x.startsWith("%env")) => {
            ("env", parseArgs(lines.mkString(" ")), "")
          }
          case x if (x.startsWith("%secret")) => {
            ("secret", parseArgs(lines.mkString(" ")), lines.drop(1).mkString("\n"))
          }
          case x if (x.startsWith("%conf")) => {
            ("conf", parseArgs(lines.mkString(" ")), "")
          }
          case x if (x.startsWith("%version")) => {
            ("version", parseArgs(lines(0)), "")
          }
          case x if (x.startsWith("%help")) => {
            ("help", parseArgs(""), "")
          }
          case _ => ("arc", collection.mutable.Map[String, String](), code.trim)
        }

        val numRows = Try(commandArgs.get("numRows").get.toInt).getOrElse(confNumRows)
        val truncate = Try(commandArgs.get("truncate").get.toInt).getOrElse(confTruncate)
        val streaming = Try(commandArgs.get("streaming").get.toBoolean).getOrElse(confStreaming)
        val streamingDuration = Try(commandArgs.get("streamingDuration").get.toInt).getOrElse(confStreamingDuration)
        val persist = Try(commandArgs.get("persist").get.toBoolean).getOrElse(false)
        val monospace = Try(commandArgs.get("monospace").get.toBoolean).getOrElse(confMonospace)
        val leftAlign = Try(commandArgs.get("leftAlign").get.toBoolean).getOrElse(confLeftAlign)
        val datasetLabels = Try(commandArgs.get("datasetLabels").get.toBoolean).getOrElse(confDatasetLabels)

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
        val lifecyclePlugins = memoizedLifecyclePlugins match {
          case Some(lifecyclePlugins) => lifecyclePlugins
          case None => {
            memoizedLifecyclePlugins = Option(ServiceLoader.load(classOf[LifecyclePlugin], loader).iterator().asScala.toList)
            memoizedLifecyclePlugins.get
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
          lifecyclePlugins=lifecyclePlugins,
          activeLifecyclePlugins=Nil,
          pipelineStagePlugins=pipelineStagePlugins,
          udfPlugins=udfPlugins,
          serializableConfiguration=new SerializableConfiguration(spark.sparkContext.hadoopConfiguration),
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
                val pipelineEither = ArcPipeline.parseConfig(Left(
                  s"""{
                    "plugins": {
                      "lifecycle": [
                        {
                          "type": "OutputTable",
                          "numRows": ${numRows},
                          "truncate": ${truncate},
                          "monospace": ${monospace},
                          "leftAlign": ${leftAlign},
                          "datasetLabels": ${datasetLabels}
                        }
                      ]
                    },
                    "stages": [${command}]
                  }""")
                  , arcContext)

                pipelineEither match {
                  case Left(errors) => ExecuteResult.Error(ai.tripl.arc.config.Error.pipelineSimpleErrorMsg(errors, false))
                  case Right((pipeline, arcCtx)) => {
                    pipeline.stages.length match {
                      case 0 => {
                        ExecuteResult.Error("No stages found.")
                      }
                      case _ => {
                        outputHandler match {
                          case Some(oh) => arcCtx.userData += ("outputHandler" -> oh)
                          case None =>
                        }
                        ARC.run(pipeline)(spark, logger, arcCtx) match {
                          case Some(df) => {
                            val result = Common.renderResult(spark, outputHandler, pipeline.stages.lastOption, df, numRows, truncate, monospace, leftAlign, datasetLabels, streamingDuration, confStreamingFrequency)
                            memoizedUserData = arcCtx.userData
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
            val df = morpheusSession.cypher(SQLUtils.injectParameters(command, confCommandLineArgs.map { case (key, config) => (key, config.value) }, true)).records.table.df
            commandArgs.get("outputView") match {
              case Some(ov) => df.createOrReplaceTempView(ov)
              case None =>
            }
            if (persist) df.persist(StorageLevel.MEMORY_AND_DISK_SER)
            ExecuteResult.Success(
              DisplayData.html(Common.renderHTML(df, None, numRows, truncate, monospace, leftAlign, datasetLabels))
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
              DisplayData.html(Common.renderHTML(df, None, numRows, truncate, monospace, leftAlign, datasetLabels))
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
              DisplayData.html(Common.renderHTML(df, None, numRows, truncate, monospace, leftAlign, datasetLabels))
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
                  confMonospace = monospace.toBoolean
                } catch {
                  case e: Exception =>
                }
              }
              case None =>
            }
            commandArgs.get("leftAlign") match {
              case Some(leftAlign) => {
                try {
                  confLeftAlign = leftAlign.toBoolean
                } catch {
                  case e: Exception =>
                }
              }
              case None =>
            }
            commandArgs.get("datasetLabels") match {
              case Some(datasetLabels) => {
                try {
                  confDatasetLabels = datasetLabels.toBoolean
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
            |Arc Options:
            |master: ${confMaster}
            |runtimeMemory: ${runtimeMemory}B
            |physicalMemory: ${physicalMemory}B
            |streaming: ${confStreaming}
            |streamingDuration: ${confStreamingDuration}
            |
            |Display Options:
            |datasetLabels: ${confDatasetLabels}
            |leftAlign: ${leftAlign}
            |monospace: ${confMonospace}
            |numRows: ${confNumRows}
            |truncate: ${confTruncate}
            """.stripMargin
            ExecuteResult.Success(
              DisplayData.text(text)
            )
          }
          case "version" => {
            ExecuteResult.Success(
              DisplayData.text(Common.getVersion)
            )
          }
          case "help" => {
            ExecuteResult.Success(
              DisplayData.text(Common.getHelp)
            )
          }
          case "list" => {
            val uri = new URI(command.trim)
            val fs = FileSystem.get(uri, spark.sparkContext.hadoopConfiguration)
            val fileStatus = fs.globStatus(new Path(uri))
            val df = fileStatus.map { file => 
              FileDisplay(
                file.getPath.getParent.toString,
                file.getPath.getName,
                Timestamp.from(Instant.ofEpochMilli(file.getModificationTime)),
                if (!file.isDirectory) FileUtils.byteCountToDisplaySize(file.getLen) else "",
                if (!file.isDirectory) file.getLen else 0
              )
            }.toSeq.toDF.orderBy(col("name"))
            commandArgs.get("outputView") match {
              case Some(ov) => df.createOrReplaceTempView(ov)
              case None =>
            }
            if (persist) df.persist(StorageLevel.MEMORY_AND_DISK_SER)
            ExecuteResult.Success(
              DisplayData.html(Common.renderHTML(df, None, numRows, truncate, monospace, leftAlign, datasetLabels))
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

  def currentLine(): Int = count
}