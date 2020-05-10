package ai.tripl.arc.jupyter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import ai.tripl.arc.api.API.ARCContext

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

  def GetHelp(): String = {
    s"""
    |Commands:
    |%arc
    |Run the stage as an Arc stage. Useful if you want to override the config settings for an individual cell.
    |Supported configuration parameters: numRows, truncate, monospace, streamingDuration
    |
    |%sql
    |Run a SQL query. 
    |Supported configuration parameters: numRows, truncate, outputView, persist, monospace, streamingDuration
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
    |numRows:           The maximum number of rows to return in a dataset (integer)
    |truncate:          The maximum number of characters displayed in a string result (integer)
    |streaming:         Set the notebook into streaming mode (boolean)
    |streamingDuration: How many seconds to execute a streaming stage before stopping (will stop if numRows is reached first).
    |monospace:         Use a fixed-width font.
    """.stripMargin
  }

  def GetVersion()(implicit spark: SparkSession, arcContext: ARCContext): String = {
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
}