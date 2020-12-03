# Change Log

## 3.10.1

- **FIX** dynamic completions where not being populated.

## 3.10.0

- add support for testing `%lifecycleplugin` via Jupyter.

## 3.9.0

- add `CONF_MAX_NUM_ROWS` environment variable to set maximum number of rows returned to reduce data exfiltration risk.
- add `%conf`, `%env` and `%schema` to the completer options.
- change completers for `%metadata`, `%printmetadata` and `%printschema` to populate available table names.
- **FIX** logic error with optional `logger`.

## 3.8.0

- add `logger` configuration (`CONF_SHOW_LOG`) option.

## 3.7.0

- add `%metadata`, `%printmetadata` and `%printschema` to the completer options.
- remove `%summary` executor in favour of `StatisticsExecute`

## 3.6.1

- add `%configexecute` support.
- update `sh.almond:kernel` to 0.10.8.
- **FIX** logic error relating to temporary placeholder views.

## 3.6.0

- update to Arc 3.4.0.

## 3.5.0

- add `CONF_STORAGE_LEVEL` to allow controlling the `persist` `storageLevel`.

## 3.4.0

- update ArcContext to support inline Schemas.
- optimisation of the `Completer` to not require the Spark API as frequently.
- change to different form of rate limited update so as to not miss events.

## 3.3.0

- make completion run asyncronously so it does not have to wait for all running stages to complete before returning completions.

## 3.2.1

- do not show anonymous views in `%sql` completer options.

## 3.2.0

- move completion logic to Arc `JupyterCompleter`, `ExtractPipelineStage`, `TransformPipelineStage` traits to allow users to define custom completions via plugins.

## 3.1.0

- intialise Spark on Kernel startup to slightly increase user experience.
- add `completer` functionality to allow code completions in JupyterLab.

## 3.0.1

- add configurations from `/opt/spark/conf/spark-defaults.conf` to simplify keeping `arc` and `arc-jupter` options in sync.

## 3.0.0

- update to Arc 3.0.0
- format timestamp as [RFC3339](https://tools.ietf.org/html/rfc3339) with nano where appropriate (omitting 'T' for [readability](https://tools.ietf.org/html/rfc3339#section-5.6)).

## 2.5.0

- add `%list` magic to display file information.
- format date and timestamps as [ISO_8601](https://en.wikipedia.org/wiki/ISO_8601) for clarity.

**NOTE** This is the last release supporting `Scala 2.11` given the release of `Spark 3.0` which only supports `Scala 2.12`.

## 2.4.2

- fix defect in setting `conf_spark_hadoop_*` variables.

## 2.4.1

- consistently log memory availablility and assigned.

## 2.4.0

- add ability to display multiple table views when execution stages which have multiple outputs
- add `%log` magic which invokes `LogExecute`
- **FIX** restore the ability for user to be able to define `fs.s3a.aws.credentials.provider` overrides via `conf_spark_hadoop_fs_s3a_aws_credentials_provider=`
- add `CONF_MASTER`, `CONF_NUM_ROWS`, `CONF_TRUNCATE`, `CONF_STREAMING_DURATION`, `CONF_STREAMING_FREQUENCY`, `CONF_DISPLAY_MONOSPACE`, `CONF_DISPLAY_LEFT_ALIGN` and `CONF_DISPLAY_DATASET_LABELS` to be able to set options via environment variables.

## 2.3.3

- modify environment variable behavior to make them easier to use for `%sql` and `%sqlvalidate` magics.

## 2.3.2

- allow environment variables directly in `%sql` and `%sqlvalidate` magics to speed development.

## 2.3.1

- only log configurations on first run after kernel start.
- prevent `math_jax` from trying to render forumulas from Arc outputs.

## 2.3.0

- force `spark.authentication` encryption for `io` (any temporary files) via `spark.io.encryption.enable` and `network` (any network traffic) via `spark.network.crypto.enabled` using a randomly generated key each notebook session.
- log configurations when starting kernel.

## 2.2.0

- add ability to switch add `text-align: left` for output tables via `leftAlign` `%conf`.
- update to Arc 2.10.0

## 2.1.1

- add ability to switch to `monospace` font for output tables via `monospace` `%conf`.
- update to Arc 2.9.0.

## 2.0.3

- change UI to treat an empty result as a `Success` outcome.

## 2.0.2

- allow any environment variable starting with `conf_` to set [Spark configuration variables](https://spark.apache.org/docs/latest/configuration.html) e.g. `conf_spark_sql_inMemoryColumnarStorage_compressed` to set `spark.sql.inMemoryColumnarStorage.compressed` (case sensitive).

## 2.0.1

- update to Arc 2.8.1 to fix some minor defects.

## 2.0.0

- update to Arc 2.8.0
- added support for inline SQL via the `%sql` and `%sqlvalidate` to align with Arc 2.8.0 changes.

## 1.10.0

- update to Arc 2.7.0

## 1.9.3

- fix defect with data display if multiple columns of same name in same dataframe.

## 1.9.2

- fix defect with streaming display

## 1.9.1

- add %secret to support safe definition of secrets
- fix issue with displaying metadata table

## 1.8.1

- update to Arc 2.4.0
- change formatting of numbers to remove scientific notation

## 1.7.1

- update to Arc 2.3.1

## 1.7.0

- change `%metadata` do show easier to read output format for any `metadata` columns.
- allow any environment variable starting with `spark_` to set [Spark configuration variables](https://spark.apache.org/docs/latest/configuration.html) e.g. `spark_sql_inMemoryColumnarStorage_compressed` to set `spark.sql.inMemoryColumnarStorage.compressed` (case sensitive).
- update to Arc 2.3.0.

## 1.6.1

- progress bar changes to support JupyterLab style.
- add plugin versions to `%version` output.

## 1.6.0

- add streaming support.
- add support for setting Spark configuration variables via environment variables
- minor UI bug fixes.

## 1.5.0

- update to Arc 2.1.0 to add `SimilarityJoinTransform`

## 1.4.0

- update to Spark 2.4.4
- update to Arc 2.0.1
- update to Scala 2.12.9
- downgrade to [almond-sh:0.6.0](https://github.com/almond-sh/almond/releases/tag/v0.6.0) to maintain Scala 2.11 compatibility

## 1.3.1

- update to [almond-sh 0.7.0](https://github.com/almond-sh/almond/releases/tag/v0.7.0)

## 1.3.0

- allow the `%env` and `%conf` magics to support multi-line inputs

## 1.2.0

- add visual progress for cypher queries

## 1.1.0

- add ability to set `numRows` and `truncate` at notebook level so they apply to all cells.

## 1.0.0

- initial release.
