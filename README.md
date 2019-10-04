Arc-Jupyter is an interactive Jupyter Notebooks Extenstion for building Arc data pipelines via Jupyter Notebooks.

## How to use

The only thing that needs to be configured is the Java Virtual Machine memory allocation which should be configured for your specific environment. e.g. to set to 4 Gigabytes:

```bash
-e JAVA_OPTS="-Xmx4096m" \
```

Here is the docker run command which exposes the Jupyter Notebook port (8888) and the Spark UI port (4040):

```bash
docker run \
-it \
--rm \
-e JAVA_OPTS="-Xmx8192m" \
--name arc-jupyter \
-p 4040:4040 \
-p 8888:8888 \
triplai/arc-jupyter:[VERSION]
```

## Capabilities

| Magic          | Description                                                                                | Scala 2.11 | Scala 2.12 | numRows | truncate | outputView | persist |
|----------------|--------------------------------------------------------------------------------------------|------------|------------|---------|----------|------------|---------|
| %help          | Display this help informaion.                                                              | ✔          | ✔          | ✔       | ✔        | ✔          |         |
| %arc           | Execute an Arc stage. Default.                                                             | ✔          | ✔          | ✔       | ✔        | ✔          |         |
| %conf          | Set configuration. Default `master=local[*]`, `numRows=20`, `truncate=50`                  | ✔          | ✔          |         |          |            |         |
| %cypher        | Execute a Cypher query and return resultset.                                               |            | ✔          | ✔       | ✔        | ✔          | ✔       |
| %env           | Set job variables via the notebook (e.g. `%env ETL_CONF_KEY0=value0 ETL_CONF_KEY1=value1`) | ✔          | ✔          |         |          |            |         |
| %metadata      | Returns the metadata of an input view as a resultset.                                      | ✔          | ✔          | ✔       | ✔        | ✔          | ✔       |
| %printmetadata | Prints the Arc metadata JSON for the input view.                                           | ✔          | ✔          |         |          |            |         |
| %printschema   | Prints the Spark schema for the input view as text.                                        | ✔          | ✔          |         |          |            |         |
| %schema        | Prints the Spark schema for the input view.                                                | ✔          | ✔          |         |          |            |         |
| %sql           | Execute a SQL query and return resultset.                                                  | ✔          | ✔          | ✔       | ✔        | ✔          | ✔       |
| %summary       | Returns the summary statistics of an input view as resultset.                              | ✔          | ✔          | ✔       | ✔        | ✔          | ✔       |
| %version       | Prints the version information of Arc Jupyter.                                             | ✔          | ✔          |         |          |            |         |


- `numRows` defines the number of rows to return in a result table.
- `truncate` defines the maximum number of characters displayed in a single result cell.
- `outputView` defines the name of a temporary view to register of the resultset.

### Example

This example shows how to use the `numRows`, `truncate` and `outputView` options:

```sql
%sql numRows=10 truncate=100 outputView=green_tripdata0
SELECT * 
FROM green_tripdata0_raw
WHERE fare_amount < 10
```

## Authors/Contributors

- [Mike Seddon](https://github.com/seddonm1)

## License

Arc-Jupyter is released under the [MIT License](https://opensource.org/licenses/MIT).

Project build with [Almond](https://github.com/almond-sh/almond) BSD 3-Clause "New" or "Revised" License.
