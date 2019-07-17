package ai.tripl.arc.jupyter

import java.util.Properties
import java.util.UUID
import java.util.ServiceLoader
import scala.collection.JavaConverters._

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
  implicit var ctx: Option[ARCContext] = None

  def kernelInfo(): KernelInfo =
    KernelInfo(
      "arc",
      ai.tripl.arc.jupyter.BuildInfo.version,
      KernelInfo.LanguageInfo(
        "arc",
        "1.0",
        "text/arc",
        "arc",
        "text" // ???
      ),
      s"""Arc kernel |Java ${sys.props.getOrElse("java.version", "[unknown]")}""".stripMargin
    )

  @volatile private var count = 0

  def execute(
    code: String,
    storeHistory: Boolean,
    inputManager: Option[InputManager],
    outputHandler: Option[OutputHandler]
  ): ExecuteResult = {
    val executionId = UUID.randomUUID.toString
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
        val session = SparkSession
          .builder()
          .master(confMaster)
          .appName("arc-jupyter")
          .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
          .config("spark.rdd.compress", true)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.driver.maxResultSize", s"${(runtimeMemorySize * 0.8).toLong}b")
          .config("spark.sql.cbo.enabled", true)
          .getOrCreate()

        spark = session

        implicit val logger = LoggerFactory.getLogger("")
        val loader = ai.tripl.arc.util.Utils.getContextOrSparkClassLoader

        // store previous value so that the ServiceLoader resolution is not called each run
        implicit val arcContext = ctx.getOrElse({
          ctx = Option(ARCContext(
            jobId=None, 
            jobName=None, 
            environment=None, 
            environmentId=None, 
            configUri=None, 
            isStreaming=false, 
            ignoreEnvironments=true, 
            commandLineArguments=Map.empty,
            storageLevel=StorageLevel.MEMORY_AND_DISK_SER,
            immutableViews=false,
            dynamicConfigurationPlugins=Nil,
            lifecyclePlugins=Nil,
            activeLifecyclePlugins=Nil,
            pipelineStagePlugins=ServiceLoader.load(classOf[PipelineStagePlugin], loader).iterator().asScala.toList,
            udfPlugins=ServiceLoader.load(classOf[UDFPlugin], loader).iterator().asScala.toList,
            userData=collection.mutable.Map.empty
          ))
          ctx.get
        })
      
        import session.implicits._

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
            ("env", parseArgs(lines(0)), "")
          } 
          case x: String if (x.startsWith("%conf")) => {
            ("conf", parseArgs(lines(0)), "")
          }                     
          case x: String if (x.startsWith("%version")) => {
            ("version", parseArgs(lines(0)), "")
          }                   
          case _ => ("arc", collection.mutable.Map[String, String](), code.trim)
        }

        outputHandler match {
          case Some(outputHandler) => {
            interpreter match {
              case "arc" | "sql" | "summary" => {
                val listener = new ProgressSparkListener(executionId)(outputHandler)         
                listener.init()(outputHandler)  
                spark.sparkContext.addSparkListener(listener)
                executionListener = Option(listener)                
              }
              case _ =>
            }
          }
          case None => None
        }        

        val numRows = commandArgs.get("numRows") match {
          case Some(numRows) => numRows.toInt
          case None => confNumRows
        }
        val truncate = commandArgs.get("truncate") match {
          case Some(truncate) => truncate.toInt
          case None => confTruncate
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
                        ExecuteResult.Success(
                          DisplayData.html(renderHTML(df, numRows, truncate))
                        )
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
            ExecuteResult.Success(
              DisplayData.html(renderHTML(df, numRows, truncate))
            )          
          }       
          case "cypher" => {
            // the morpheus session must be created by the GraphTransform stage
            val morpheus = arcContext.userData.get("morpheusSession") match {
              case Some(m: MorpheusSession) => m
              case _ => {
                throw new Exception(s"CypherTransform executes an existing graph created with GraphTransform but no session exists to execute CypherTransform against.")
              }
            }   
            val df = morpheus.cypher(SQLUtils.injectParameters(command, arcContext.commandLineArguments, true)).records.table.df
            commandArgs.get("outputView") match {
              case Some(ov) => df.createOrReplaceTempView(ov)
              case None =>
            }
            ExecuteResult.Success(
              DisplayData.html(renderHTML(df, numRows, truncate))
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
            val df = MetadataUtils.createMetadataDataframe(spark.table(command))
            commandArgs.get("outputView") match {
              case Some(ov) => df.createOrReplaceTempView(ov)
              case None =>
            }
            ExecuteResult.Success(
              DisplayData.html(renderHTML(df, numRows, truncate))
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
            ExecuteResult.Success(
              DisplayData.html(renderHTML(df, numRows, truncate))
            )   
          }           
          case "env" => {
            // recreate context with the new environment
            ctx = Option(ARCContext(
              jobId=arcContext.jobId, 
              jobName=arcContext.jobName, 
              environment=arcContext.environment, 
              environmentId=arcContext.environmentId, 
              configUri=arcContext.configUri, 
              isStreaming=arcContext.isStreaming, 
              ignoreEnvironments=arcContext.ignoreEnvironments, 
              storageLevel=arcContext.storageLevel,
              immutableViews=arcContext.immutableViews,
              commandLineArguments=commandArgs.toMap,
              dynamicConfigurationPlugins=arcContext.dynamicConfigurationPlugins,
              lifecyclePlugins=arcContext.lifecyclePlugins,
              activeLifecyclePlugins=arcContext.activeLifecyclePlugins,
              pipelineStagePlugins=arcContext.pipelineStagePlugins,
              udfPlugins=arcContext.udfPlugins,
              userData=arcContext.userData
            ))
            ExecuteResult.Success(DisplayData.empty)     
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
            ExecuteResult.Success(
              DisplayData.text(s"master: ${confMaster}\nnumRows: ${confNumRows}\ntruncate: ${confTruncate}\n")
            )  
          }              
          case "version" => {
            ExecuteResult.Success(
              DisplayData.text(s"spark: ${spark.version}\narc: ${ai.tripl.arc.ArcBuildInfo.BuildInfo.version}\narc-jupyter: ${ai.tripl.arc.jupyter.BuildInfo.version}\nscala: ${scala.util.Properties.versionNumberString}\njava: ${System.getProperty("java.runtime.version")}")
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
          listener.update(true)(outputHandler.get)
        }
        spark.sparkContext.removeSparkListener(listener)
      }
      case None =>
    }
  }
  
  def currentLine(): Int =
    count

  def renderHTML(df: DataFrame, numRows: Int, truncate: Int): String = {
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

    s""" 
    <table>
        <tr>
          ${header.map(h => s"<th>${escape(h)}</th>").mkString}
        </tr>
        ${_rows.map { r =>
          s"<tr>${r.map { c => s"<td>${escape(c)}</td>" }.mkString}</tr>"
        }.mkString}
    </table>
    """
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