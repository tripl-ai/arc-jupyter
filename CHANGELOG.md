## Change Log

# 1.9.2

- fix defect with streaming display

# 1.9.1

- add %secret to support safe definition of secrets
- fix issue with displaying metadata table

# 1.8.1

- update to Arc 2.4.0
- change formatting of numbers to remove scientific notation

# 1.7.1

- update to Arc 2.3.1

# 1.7.0

- change `%metadata` do show easier to read output format for any `metadata` columns.
- allow any environment variable starting with `spark_` to set [Spark configuration variables](https://spark.apache.org/docs/latest/configuration.html) e.g. `spark_sql_inMemoryColumnarStorage_compressed` to set `spark.sql.inMemoryColumnarStorage.compressed` (case sensitive).
- update to Arc 2.3.0.

# 1.6.1

- progress bar changes to support JupyterLab style.
- add plugin versions to `%version` output.

# 1.6.0

- add streaming support.
- add support for setting Spark configuration variables via environment variables
- minor UI bug fixes.

# 1.5.0

- update to Arc 2.1.0 to add `SimilarityJoinTransform`

# 1.4.0

- update to Spark 2.4.4
- update to Arc 2.0.1
- update to Scala 2.12.9
- downgrade to `almond-sh` 0.6.0 https://github.com/almond-sh/almond/releases/tag/v0.6.0


# 1.3.1

- update to `almond-sh` 0.7.0 https://github.com/almond-sh/almond/releases/tag/v0.7.0

# 1.3.0

- allow the `%env` and `%conf` magics to support multi-line inputs

# 1.2.0

- add visual progress for cypher queries

# 1.1.0

- add ability to set `numRows` and `truncate` at notebook level so they apply to all cells.

# 1.0.0

- initial release.