package ai.tripl.arc.jupyter

object Common {

  def GetHelp(): String = {
    s"""
    |Commands:
    |%arc
    |Run the stage as an Arc stage. Useful if you want to override the config settings for an individual cell.
    |Supported configuration parameters: numRows, truncate, streamingDuration
    |
    |%sql
    |Run a SQL query. 
    |Supported configuration parameters: numRows, truncate, outputView, persist, streamingDuration
    |
    |%cypher
    |Run a Cypher graph query. Scala 2.12 only.
    |Supported configuration parameters: numRows, truncate, outputView, persist
    |
    |%schema [view]
    |Display a JSON formatted schema for the input view
    |
    |%printschema [view]
    |Display a printable basic schema for the input view
    |
    |%metadata [view]
    |Create an Arc metadata dataset for the input view
    |Supported configuration parameters: numRows, truncate, outputView, persist
    |
    |%printmetadata [view]
    |Display a JSON formatted Arc metadata schema for the input view
    |
    |%summary [view]
    |Create an Summary statistics dataset for the input view
    |Supported configuration parameters: numRows, truncate, outputView, persist
    |
    |%env
    |Set variables for this session. E.g. ETL_CONF_BASE_DIR=/home/jovyan/tutorial
    |Supported configuration parameters: numRows, truncate, outputView, persist, streamingDuration
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
    """.stripMargin
  }
}