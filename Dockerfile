FROM jupyter/pyspark-notebook:abdb27a6dfbb

ARG ARC_JUPYTER_VERSION
ENV SCALA_VERSION         2.11
ENV JAVA_OPTS "-Xmx1g"
ENV BASE_JAVA_OPTS "-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap"

# add the execution time extension
RUN pip install jupyter_contrib_nbextensions && \
  jupyter contrib nbextension install --user && \
  jupyter nbextensions_configurator disable --user && \
  jupyter nbextension enable execute_time/ExecuteTime

# build and add plugin
USER jovyan
RUN cd /tmp && \
  wget -P /tmp https://git.io/coursier-cli && \
  chmod +x /tmp/coursier-cli && \
  /tmp/coursier-cli bootstrap \
  --embed-files=false \
  --force-version com.fasterxml.jackson.core:jackson-databind:2.6.7.1 \
  --force-version org.json4s:json4s-ast_${SCALA_VERSION}:3.5.3 \
  --force-version org.json4s:json4s-core_${SCALA_VERSION}:3.5.3 \
  --force-version org.json4s:json4s-jackson_${SCALA_VERSION}:3.5.3 \
  --force-version org.json4s:json4s-scalap_${SCALA_VERSION}:3.5.3 \
  --force-version com.google.guava:guava:14.0.1 \
  --force-version org.slf4j:slf4j-log4j12:1.7.16 \
  --exclude org.slf4j:slf4j-nop \
  ai.tripl:arc-jupyter_${SCALA_VERSION}:${ARC_JUPYTER_VERSION} \
  -o arc && \
  ./arc --install --force && \
  rm /tmp/arc && \
  rm /tmp/coursier-cli

# override default run command to allow JAVA_OPTS injection
COPY kernel.json /home/jovyan/.local/share/jupyter/kernels/arc/kernel.json

# add Download as: Arc (.json) to jupyter notebook
RUN pip install git+https://github.com/tripl-ai/nb_extension_arcexport.git

# ui tweaks
# css to maximise screen realestate
COPY custom.css /home/jovyan/.jupyter/custom/custom.css
# js to set code formatting for arc commmands
COPY custom.js /home/jovyan/.jupyter/custom/custom.js

# disable default save data with notebooks
COPY scrub_output_pre_save.py /tmp/scrub_output_pre_save.py
RUN cat /tmp/scrub_output_pre_save.py >> /etc/jupyter/jupyter_notebook_config.py
