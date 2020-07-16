package ai.tripl.arc.jupyter

import java.security.SecureRandom
import java.lang.management.ManagementFactory
import java.io._
import java.util.Properties
import java.nio.charset.StandardCharsets

import util.control.Breaks._
import scala.collection.JavaConverters._

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

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.node._

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
    |%list
    |Show a list of files in a target directory
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
        // will convert to stringtype with UTC ZoneID
        case TimestampType => concat(col(fieldName).cast(StringType),lit("Z"))
        case DateType => date_format(col(fieldName), "yyyy-MM-dd")
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

  case class Completion (
    text: String,
    textType: String,
    replaceText: String,
    language: String,
    documentation: String
  )

  // todo: these should come from traits in arc
  def getCompletions()(implicit spark: SparkSession, arcContext: ARCContext): (Seq[String], RawJson) = {
    val completions = Seq(
      Completion(
        "AvroExtract",
        "extract",
        """{
        |  "type": "AvroExtract",
        |  "name": "AvroExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.avro",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#avroextract"
      ),
      Completion(
        "BytesExtract",
        "extract",
        """{
        |  "type": "BytesExtract",
        |  "name": "BytesExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.jpg",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#bytesextract"
      ),
      Completion(
        "DeltaLakeExtract",
        "extract",
        """{
        |  "type": "DeltaLakeExtract",
        |  "name": "DeltaLakeExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.delta",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#deltalakeextract"
      ),
      Completion(
        "DelimitedExtract",
        "extract",
        """{
        |  "type": "DelimitedExtract",
        |  "name": "DelimitedExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.csv",
        |  "outputView": "outputView",
        |  "header": false
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#delimitedextract"
      ),
      Completion(
        "HTTPExtract",
        "extract",
        """{
        |  "type": "HTTPExtract",
        |  "name": "HTTPExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "https://",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#httpextract"
      ),
      Completion(
        "ImageExtract",
        "extract",
        """{
        |  "type": "ImageExtract",
        |  "name": "ImageExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.jpg",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#imageextract"
      ),
      Completion(
        "JDBCExtract",
        "extract",
        """{
        |  "type": "JDBCExtract",
        |  "name": "JDBCExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "jdbc:postgresql://",
        |  "tableName": "(SELECT * FROM ) outputView",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#jdbcextract"
      ),
      Completion(
        "JSONExtract",
        "extract",
        """{
        |  "type": "JSONExtract",
        |  "name": "JSONExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.json",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#jsonextract"
      ),
      Completion(
        "KafkaExtract",
        "extract",
        """{
        |  "type": "KafkaExtract",
        |  "name": "KafkaExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "bootstrapServers": "kafka:9092",
        |  "topic": "topic",
        |  "groupID": "groupId",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#kafkaextract"
      ),
      Completion(
        "MetadataExtract",
        "extract",
        """{
        |  "type": "MetadataExtract",
        |  "name": "MetadataExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#metadataextract"
      ),
      Completion(
        "MongoExtract",
        "extract",
        """{
        |  "type": "MongoExtract",
        |  "name": "MongoExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "options": {
        |    "uri": "mongodb://username:password@mongo:27017",
        |    "database": "database",
        |    "collection": "collection",
        |  },
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#mongoextract"
      ),
      Completion(
        "ORCExtract",
        "extract",
        """{
        |  "type": "ORCExtract",
        |  "name": "ORCExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.orc",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#orcextract"
      ),
      Completion(
        "ParquetExtract",
        "extract",
        """{
        |  "type": "ParquetExtract",
        |  "name": "ParquetExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.parquet",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#parquetextract"
      ),
      Completion(
        "RateExtract",
        "extract",
        """{
        |  "type": "RateExtract",
        |  "name": "RateExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "rowsPerSecond": 1,
        |  "rampUpTime": 0,
        |  "numPartitions": 10,
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#rateextract"
      ),
      Completion(
        "SASExtract",
        "extract",
        """{
        |  "type": "SASExtract",
        |  "name": "SASExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.sas7bdat",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#sasextract"
      ),
      Completion(
        "TextExtract",
        "extract",
        """{
        |  "type": "TextExtract",
        |  "name": "TextExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.txt",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#textextract"
      ),
      Completion(
        "XMLExtract",
        "extract",
        """{
        |  "type": "XMLExtract",
        |  "name": "XMLExtract",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.xml",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/extract/#xmlextract"
      ),
      Completion(
        "DiffTransform",
        "transform",
        """{
        |  "type": "DiffTransform",
        |  "name": "DiffTransform",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputLeftView": "inputLeftView",
        |  "inputRightView": "inputRightView",
        |  "outputLeftView": "outputLeftView",
        |  "outputIntersectionView": "outputIntersectionView",
        |  "outputRightView": "outputRightView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/transform/#difftransform"
      ),
      Completion(
        "HTTPTransform",
        "transform",
        """{
        |  "type": "HTTPTransform",
        |  "name": "HTTPTransform",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "uri": "https://",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/transform/#httptransform"
      ),
      Completion(
        "JSONTransform",
        "transform",
        """{
        |  "type": "JSONTransform",
        |  "name": "JSONTransform",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/transform/#jsontransform"
      ),
      Completion(
        "MetadataFilterTransform",
        "transform",
        """{
        |  "type": "MetadataFilterTransform",
        |  "name": "MetadataFilterTransform",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "inputURI": "hdfs://*.sql",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/transform/#metadatafiltertransform"
      ),
      Completion(
        "MetadataTransform",
        "transform",
        """{
        |  "type": "MetadataTransform",
        |  "name": "MetadataTransform",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputView": "outputView",
        |  "schemaURI": "hdfs://*.json"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/transform/#metadatatransform"
      ),
      Completion(
        "MLTransform",
        "transform",
        """{
        |  "type": "MLTransform",
        |  "name": "MLTransform",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "inputURI": "hdfs://*.model",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/transform/#mltransform"
      ),
      Completion(
        "SimilarityJoinTransform",
        "transform",
        """{
        |  "type": "SimilarityJoinTransform",
        |  "name": "SimilarityJoinTransform",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "threshold": 0.75,
        |  "leftView": "leftView",
        |  "leftFields": [],
        |  "rightView": "rightView",
        |  "rightFields": [],
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/transform/#similarityjointransform"
      ),
      Completion(
        "SQLTransform",
        "transform",
        """{
        |  "type": "SQLTransform",
        |  "name": "SQLTransform",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.sql",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/transform/#sqltransform"
      ),
      Completion(
        "%sql",
        "transform",
        if (arcContext == null) {
          """%sql name="sqlvalidate" outputView=outputView environments=production,test
            |SELECT
            |  *
            |FROM inputView""".stripMargin
        } else {
          arcContext.userData.get("lastView").flatMap { lastView =>
            try {
              lastView.asInstanceOf[Option[String]]
            } catch {
              case e: Exception => None
            }
          } match {
            case None => {
              """%sql name="sqlvalidate" outputView=outputView environments=production,test
              |SELECT
              |  *
              |FROM inputView""".stripMargin
            }
            case Some(lastView) => {
              val df = spark.table(lastView)
              df.columns
              s"""%sql name="sqlvalidate" outputView=outputView environments=production,test
              |SELECT
              |${df.columns.map { col => if (col.indexOf(" ") == -1) col else s"`$col`" }.mkString("  ", "\n  ,", "")}
              |FROM ${lastView}""".stripMargin
            }
          }
        },
        "sql",
        "https://arc.tripl.ai/transform/#sqltransform"
      ),
      Completion(
        "TensorFlowServingTransform",
        "transform",
        """{
        |  "type": "TensorFlowServingTransform",
        |  "name": "TensorFlowServingTransform",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "signatureName": "serving_default",
        |  "inputView": "inputView",
        |  "inputURI": "hdfs://*.sql",
        |  "outputView": "outputView",
        |
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/transform/#tensorflowservingtransform"
      ),

      Completion(
        "TypingTransform",
        "transform",
        """{
        |  "type": "TypingTransform",
        |  "name": "TypingTransform",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "schemaURI": "hdfs://*.json",
        |  "outputView": "outputView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/transform/#typingtransform"
      ),
      Completion(
        "AvroLoad",
        "load",
        """{
        |  "type": "AvroLoad",
        |  "name": "AvroLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputURI": "hdfs://*.avro"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#avroload"
      ),
      Completion(
        "DeltaLakeLoad",
        "load",
        """{
        |  "type": "DeltaLakeLoad",
        |  "name": "DeltaLakeLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputURI": "hdfs://*.delta"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#deltalakeload"
      ),
      Completion(
        "DeltaLakeMergeLoad",
        "load",
        """{
        |  "type": "DeltaLakeMergeLoad",
        |  "name": "DeltaLakeMergeLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputURI": "hdfs://*.delta",
        |  "condition": "source.primaryKey = target.primaryKey",
        |  "whenNotMatchedByTargetInsert": {},
        |  "whenNotMatchedBySourceDelete": {}
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#deltalakemergeload"
      ),
      Completion(
        "DelimitedLoad",
        "load",
        """{
        |  "type": "DelimitedLoad",
        |  "name": "DelimitedLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputURI": "hdfs://*.csv"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#delimitedload"
      ),
      Completion(
        "HTTPLoad",
        "load",
        """{
        |  "type": "HTTPLoad",
        |  "name": "HTTPLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputURI": "https://"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#httpload"
      ),
      Completion(
        "JDBCLoad",
        "load",
        """{
        |  "type": "JDBCLoad",
        |  "name": "JDBCLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "jdbcURL": "jdbc:postgresql://",
        |  "tableName": "database.schema.table"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#jdbcload"
      ),
      Completion(
        "JSONLoad",
        "load",
        """{
        |  "type": "JSONLoad",
        |  "name": "JSONLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputURI": "hdfs://*.json"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#jsonload"
      ),
      Completion(
        "KafkaLoad",
        "load",
        """{
        |  "type": "KafkaLoad",
        |  "name": "KafkaLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "bootstrapServers": "kafka:9092",
        |  "topic": "topic"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#kafkaload"
      ),
      Completion(
        "MongoLoad",
        "load",
        """{
        |  "type": "MongoLoad",
        |  "name": "MongoLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "options": {
        |    "uri": "mongodb://username:password@mongo:27017",
        |    "database": "database",
        |    "collection": "collection",
        |  }
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#mongoload"
      ),
      Completion(
        "ORCLoad",
        "load",
        """{
        |  "type": "ORCLoad",
        |  "name": "ORCLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputURI": "hdfs://*.orc"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#orcload"
      ),
      Completion(
        "ParquetLoad",
        "load",
        """{
        |  "type": "ParquetLoad",
        |  "name": "ParquetLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputURI": "hdfs://*.parquet"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#parquetload"
      ),
      Completion(
        "TextLoad",
        "load",
        """{
        |  "type": "TextLoad",
        |  "name": "TextLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputURI": "hdfs://*.txt"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#textload"
      ),
      Completion(
        "XMLLoad",
        "load",
        """{
        |  "type": "XMLLoad",
        |  "name": "XMLLoad",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "outputURI": "hdfs://*.xml"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/load/#xmlload"
      ),
      Completion(
        "HTTPExecute",
        "execute",
        """{
        |  "type": "HTTPExecute",
        |  "name": "HTTPExecute",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "uri": "https://",
        |  "headers": {}
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/execute/#httpexecute"
      ),
      Completion(
        "JDBCExecute",
        "execute",
        """{
        |  "type": "JDBCExecute",
        |  "name": "JDBCExecute",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.sql",
        |  "jdbcURL": "jdbc:postgresql://"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/execute/#jdbcexecute"
      ),
      Completion(
        "KafkaCommitExecute",
        "execute",
        """{
        |  "type": "KafkaCommitExecute",
        |  "name": "KafkaCommitExecute",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "bootstrapServers": "kafka:9092",
        |  "groupID": "groupID"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/execute/#kafkacommitexecute"
      ),
      Completion(
        "LogExecute",
        "execute",
        """{
        |  "type": "LogExecute",
        |  "name": "LogExecute",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.sql"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/execute/#logexecute"
      ),
      Completion(
        "%log",
        "execute",
        """%log name="log" environments=production,test
        |SELECT
        |  TO_JSON(
        |    NAMED_STRUCT(
        |      'key', 'value'
        |    )
        |  ) AS message""".stripMargin,
        "sql",
        "https://arc.tripl.ai/execute/#logexecute"
      ),
      Completion(
        "PipelineExecute",
        "execute",
        """{
        |  "type": "PipelineExecute",
        |  "name": "PipelineExecute",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "uri": "hdfs://*.json"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/execute/#pipelineexecute"
      ),
      Completion(
        "EqualityValidate",
        "validate",
        """{
        |  "type": "EqualityValidate",
        |  "name": "EqualityValidate",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "leftView": "leftView",
        |  "rightView": "rightView"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/validate/#equalityvalidate"
      ),
      Completion(
        "MetadataValidate",
        "validate",
        """{
        |  "type": "MetadataValidate",
        |  "name": "MetadataValidate",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputView": "inputView",
        |  "inputURI": "hdfs://*.sql"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/validate/#metadatavalidate"
      ),
      Completion(
        "SQLValidate",
        "validate",
        """{
        |  "type": "SQLValidate",
        |  "name": "SQLValidate",
        |  "environments": [
        |    "production",
        |    "test"
        |  ],
        |  "inputURI": "hdfs://*.sql"
        |}""".stripMargin,
        "javascript",
        "https://arc.tripl.ai/validate/#sqlvalidate"
      ),
      Completion(
        "%sqlvalidate",
        "validate",
        """%sqlvalidate name="sqlvalidate" environments=production,test
        |SELECT
        |  TRUE AS valid
        |  ,TO_JSON(
        |    NAMED_STRUCT(
        |      'key', 'value'
        |    )
        |  ) AS message""".stripMargin,
        "sql",
        "https://arc.tripl.ai/validate/#sqlvalidate"
      )
    )

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

    (completions.map(_.text), RawJson(objectMapper.writeValueAsString(node).getBytes(StandardCharsets.UTF_8)))
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

}