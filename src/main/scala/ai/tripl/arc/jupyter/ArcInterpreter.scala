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
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.Properties._
import scala.util.Try
import util.control.Breaks._

import almond.interpreter.{Completion, ExecuteResult, Inspection, Interpreter}
import almond.interpreter.api.{DisplayData, OutputHandler}
import almond.interpreter.input.InputManager
import almond.interpreter.util.CancellableFuture
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

import org.apache.commons.lang3.exception.ExceptionUtils

import com.typesafe.config._

import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.ARC
import ai.tripl.arc.config.{ArcPipeline, ConfigUtils}
import ai.tripl.arc.plugins._
import ai.tripl.arc.util.log.LoggerFactory
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.SerializableConfiguration
import ai.tripl.arc.util.SQLUtils

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
  implicit var arcContext: ARCContext = _

  // the memory available to the container (i.e. the docker memory limit)
  val physicalMemory = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean].getTotalPhysicalMemorySize
  // the JVM requested memory (-Xmx)
  val runtimeMemory = Runtime.getRuntime.maxMemory

  val authenticateSecret = Common.randStr(64)

  val secretPattern = """"(token|signature|accessKey|secret|secretAccessKey)":[\s]*".*"""".r

  var confCommandLineArgs: Map[String, ConfigValue] = Map.empty
  var confMaster = envOrNone("CONF_MASTER").getOrElse("local[*]")
  var confNumRows = Try(envOrNone("CONF_NUM_ROWS").get.toInt).getOrElse(20)
  var confTruncate = Try(envOrNone("CONF_TRUNCATE").get.toInt).getOrElse(50)
  var confStreamingDuration = Try(envOrNone("CONF_STREAMING_DURATION").get.toInt).getOrElse(10)
  var confStreamingFrequency = Try(envOrNone("CONF_STREAMING_FREQUENCY").get.toInt).getOrElse(1000)
  var confMonospace = Try(envOrNone("CONF_DISPLAY_MONOSPACE").get.toBoolean).getOrElse(false)
  var confLeftAlign = Try(envOrNone("CONF_DISPLAY_LEFT_ALIGN").get.toBoolean).getOrElse(false)
  var confDatasetLabels = Try(envOrNone("CONF_DISPLAY_DATASET_LABELS").get.toBoolean).getOrElse(false)
  var confExtendedErrors = Try(envOrNone("CONF_DISPLAY_EXTENDED_ERRORS").get.toBoolean).getOrElse(false)
  var policyInlineSQL = Try(envOrNone("ETL_POLICY_INLINE_SQL").get.toBoolean).getOrElse(true)
  var policyInlineSchema = Try(envOrNone("ETL_POLICY_INLINE_SCHEMA").get.toBoolean).getOrElse(true)
  var confStreaming = false
  var udfsRegistered = false

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
        "javascript",
        ".json",
        "arcexport",
        None,
        Some("javascript")
      ),
      s"""arc-jupyter ${ai.tripl.arc.jupyter.BuildInfo.version} arc ${ai.tripl.arc.util.Utils.getFrameworkVersion}"""".stripMargin
    )

  @volatile private var count = 0

  override def init(): Unit = {
    startSession()
  }

  override def asyncComplete(code: String, pos: Int): Option[CancellableFuture[Completion]] = {
    val spaceIndex = code.indexOf(" ")
    if (spaceIndex == -1 || (pos < spaceIndex)){
      val c = Common.getCompletions(pos, code.length)
      Some(CancellableFuture(Future.successful(c), () => sys.error("should not happen")))
    } else {
      None
    }
  }

  def startSession(): SparkSession = {
    val emptySession = SparkSession.getActiveSession.isEmpty

    if (emptySession) {
      val sessionBuilder = SparkSession
        .builder()
        .master(confMaster)
        .appName("arc-jupyter")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.authenticate.secret", authenticateSecret)
        .config("spark.driver.maxResultSize", s"${(runtimeMemory * 0.8).toLong}B")
        .config("spark.scheduler.mode", "FAIR")

      // read the defaults from spark-defaults.conf
      Common.getPropertiesFromFile("/opt/spark/conf/spark-defaults.conf")
        .filter { case (key, _) => key.startsWith("spark.") }
        .foreach { case (key, value) => {
          sessionBuilder.config(key, value)
        }}

      // add any spark overrides
      System.getenv.asScala
        .filter { case (key, _) => key.startsWith("conf_spark") }
        // apply hadoop options after spark session creation
        .filter { case (key, _) => !key.startsWith("conf_spark_hadoop") }
        // you cannot override these settings for security
        .filter { case (key, _) => !Seq("conf_spark_authenticate", "conf_spark_authenticate_secret", "conf_spark_io_encryption_enable", "conf_spark_network_crypto_enabled").contains(key) }
        .foreach{ case (key: String, value: String) => {
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
        .field("policyInlineSQL", policyInlineSQL.toString)
        .field("policyInlineSchema", policyInlineSchema.toString)
        .log()

      // only set default aws provider override if not provided
      if (Option(spark.sparkContext.hadoopConfiguration.get("fs.s3a.aws.credentials.provider")).isEmpty) {
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", ai.tripl.arc.util.CloudUtils.defaultAWSProvidersOverride)
      }

      // start the sqlcontext
      spark.sql("""SELECT TRUE""")
    }

    SparkSession.getActiveSession.get
  }

  def execute(
    code: String,
    storeHistory: Boolean,
    inputManager: Option[InputManager],
    outputHandler: Option[OutputHandler]
  ): ExecuteResult = {
    val listenerElementHandle = Common.randStr(32)
    var executionListener: Option[ProgressSparkListener] = None

    try {
      val executeResult = if (runtimeMemory > physicalMemory) {
        return ExecuteResult.Error(s"Cannot execute as requested JVM memory (-Xmx${FileUtils.byteCountToDisplaySize(runtimeMemory)}B) exceeds available system memory (${FileUtils.byteCountToDisplaySize(physicalMemory)}B) limit.\nEither decrease the requested JVM memory or, if running in Docker, increase the Docker memory limit.")
      } else if (spark == null) {
        return ExecuteResult.Error(s"SparkSession has not been initialised. Please restart Kernel or wait for startup completion.")
      } else {
        implicit val logger = LoggerFactory.getLogger("arc-jupyter")

        // if session config changed and session stopped
        val session = startSession()
        import session.implicits._

        val loader = ai.tripl.arc.util.Utils.getContextOrSparkClassLoader

        // parse input
        val lines = code.trim.split("\n")
        val (interpreter, commandArgs, command, deregister) = lines(0) match {
          case x if x.startsWith("%arc") => ("arc", parseArgs(lines(0)), lines.drop(1).mkString("\n"), None)
          // outputs which may have outputView
          case x if (x.startsWith("%sql") && !x.startsWith("%sqlvalidate")) || x.startsWith("%metadatafilter") => {
            val commandArgs = parseArgs(lines(0))
            commandArgs.get("outputView") match {
              case None => {
                val rnd = Common.randStr(32)
                ("arc", commandArgs, s"""${lines(0)} outputView=${rnd}\n${lines.drop(1).mkString("\n")}""", Option(rnd))
              }
              case Some(_) => ("arc", commandArgs, lines.mkString("\n"), None)
            }
          }
          case x if x.startsWith("%metadatavalidate") || x.startsWith("%sqlvalidate") => {
            val commandArgs = parseArgs(lines(0))
            ("arc", commandArgs, lines.mkString("\n"), None)
          }
          case x if (x.startsWith("%configplugin")) => {
            ("configplugin", parseArgs(lines(0)), lines.drop(1).mkString("\n"), None)
          }
          case x if (x.startsWith("%lifecycleplugin")) => {
            ("lifecycleplugin", parseArgs(lines(0)), lines.drop(1).mkString("\n"), None)
          }
          case x if (x.startsWith("%schema")) => {
            ("schema", parseArgs(lines(0)), lines.drop(1).mkString("\n"), None)
          }
          case x if (x.startsWith("%printschema")) => {
            ("printschema", parseArgs(lines(0)), lines.drop(1).mkString("\n"), None)
          }
          case x if (x.startsWith("%metadata")) => {
            ("metadata", parseArgs(lines(0)), lines.drop(1).mkString("\n"), None)
          }
          case x if (x.startsWith("%printmetadata")) => {
            ("printmetadata", parseArgs(lines(0)), lines.drop(1).mkString("\n"), None)
          }
          case x if (x.startsWith("%summary")) => {
            ("summary", parseArgs(lines(0)), lines.drop(1).mkString("\n"), None)
          }
          case x if (x.startsWith("%list")) => {
            ("list", parseArgs(lines(0)), lines.drop(1).mkString("\n"), None)
          }
          case x if (x.startsWith("%env")) => {
            ("env", parseArgs(lines.mkString(" ")), "", None)
          }
          case x if (x.startsWith("%secret")) => {
            ("secret", parseArgs(lines.mkString(" ")), lines.drop(1).mkString("\n"), None)
          }
          case x if (x.startsWith("%conf")) => {
            ("conf", parseArgs(lines.mkString(" ")), "", None)
          }
          case x if (x.startsWith("%version")) => {
            ("version", parseArgs(lines(0)), "", None)
          }
          case x if (x.startsWith("%help")) => {
            ("help", parseArgs(""), "", None)
          }
          case _ => ("arc", collection.mutable.Map[String, String](), code.trim, None)
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

        arcContext = ARCContext(
          jobId=None,
          jobName=None,
          environment=None,
          configUri=None,
          isStreaming=confStreaming,
          ignoreEnvironments=true,
          commandLineArguments=confCommandLineArgs.map { case (key, config) => (key, config.value) },
          storageLevel=StorageLevel.MEMORY_AND_DISK_SER,
          immutableViews=false,
          dropUnsupported=false,
          dynamicConfigurationPlugins=dynamicConfigsPlugins,
          lifecyclePlugins=lifecyclePlugins,
          activeLifecyclePlugins=Nil,
          pipelineStagePlugins=pipelineStagePlugins,
          udfPlugins=udfPlugins,
          serializableConfiguration=new SerializableConfiguration(spark.sparkContext.hadoopConfiguration),
          userData=memoizedUserData,
          ipynb=true,
          inlineSchema=policyInlineSchema,
          inlineSQL=policyInlineSQL,
        )

        // register udfs once
        if (!udfsRegistered) {
          ai.tripl.arc.udf.UDF.registerUDFs()(spark, logger, arcContext)
          udfsRegistered = true
        }

        outputHandler match {
          case Some(outputHandler) => {
            interpreter match {
              case "arc" | "summary" => {
                val listener = new ProgressSparkListener(listenerElementHandle)(outputHandler, logger)
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
                val (_, _, stages) = ConfigUtils.parseIPYNBCells(List(command))

                print(stages)
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
                    "stages": [${stages}]
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
                            deregister.foreach { viewName => {
                              try {
                                spark.catalog.dropTempView(viewName.toLowerCase())
                              } catch {
                                case e: Exception =>
                              }
                            }}
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
            confNumRows = Try(commandArgs.get("numRows").get.toInt).getOrElse(confNumRows)
            confTruncate = Try(commandArgs.get("truncate").get.toInt).getOrElse(confTruncate)
            confStreaming = Try(commandArgs.get("streaming").get.toBoolean).getOrElse(confStreaming)
            confStreamingDuration = Try(commandArgs.get("streamingDuration").get.toInt).getOrElse(confStreamingDuration)
            confMonospace = Try(commandArgs.get("monospace").get.toBoolean).getOrElse(confMonospace)
            confLeftAlign = Try(commandArgs.get("leftAlign").get.toBoolean).getOrElse(confLeftAlign)
            confDatasetLabels = Try(commandArgs.get("datasetLabels").get.toBoolean).getOrElse(confDatasetLabels)
            confExtendedErrors = Try(commandArgs.get("extendedErrors").get.toBoolean).getOrElse(confExtendedErrors)

            val text = s"""
            |Arc Options:
            |master: ${confMaster}
            |runtimeMemory: ${runtimeMemory}B
            |physicalMemory: ${physicalMemory}B
            |streaming: ${confStreaming}
            |streamingDuration: ${confStreamingDuration}
            |
            |Display Options:
            |extendedErrors: ${confExtendedErrors}
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
            val fileStatus = fs.listStatus(new Path(uri))
            val df = fileStatus.map { file =>
              FileDisplay(
                Option(file.getPath.getParent).getOrElse("/").toString,
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
        if (confExtendedErrors) {
          val exceptionThrowables = ExceptionUtils.getThrowableList(e).asScala
          val exceptionThrowablesMessages = exceptionThrowables.map(e => e.getMessage).mkString("\n\n")
          ExecuteResult.Error(exceptionThrowablesMessages)
        } else {
          ExecuteResult.Error(e.getMessage)
        }
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