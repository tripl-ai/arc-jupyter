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

## Authors/Contributors

- [Mike Seddon](https://github.com/seddonm1)

## License

Arc-Jupyter is released under the [MIT License](https://opensource.org/licenses/MIT).

Project build with [Almond](https://github.com/almond-sh/almond) BSD 3-Clause "New" or "Revised" License.
