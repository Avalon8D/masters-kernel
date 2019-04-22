#!/bin/bash

# dataproc small example
# gcloud beta dataproc clusters create thesis-tiny-python3-anaconda --subnet default --zone us-central1-a --master-machine-type n1-highmem-2 --master-boot-disk-size 40 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 40 --image-version 1.4-debian9 --project seo-analytics-219818 --optional-components=ANACONDA --initialization-actions 'gs://<bucket>/<path-to-initialization-action>/python3_dataproc.sh' --initialization-action-timeout 20m

echo "export PYTHONHASHSEED=0
export AWS_SECRET_ACCESS_KEY=<access_key>
export AWS_ACCESS_KEY_ID=<key_id>
export PYSPARK_DRIVER_PYTHON=/opt/python3/bin/python
" | \
tee -a /etc/profile.d/spark_config.sh /etc/*bashrc /usr/lib/spark/conf/spark-env.sh

echo "spark.executorEnv.PYTHONHASHSEED=0
spark.executorEnv.AWS_SECRET_ACCESS_KEY=<access_key>
spark.executorEnv.AWS_ACCESS_KEY_ID=<key_id>
export PYSPARK_DRIVER_PYTHON=/opt/python3/bin/python
" >> /etc/spark/conf/spark-defaults.conf

echo "$(head -n -1 /usr/lib/spark/bin/spark-submit)""export AWS_SECRET_ACCESS_KEY=<access_key>
export AWS_ACCESS_KEY_ID=<key_id>
export PYSPARK_DRIVER_PYTHON=/opt/python3/bin/python
""$(tail -n 1 /usr/lib/spark/bin/spark-submit)" >> /usr/lib/spark/bin/spark-submit

head /usr/lib/spark/bin/pyspark -n -3 > /usr/bin/jupyspark
echo 'export PYSPARK_DRIVER_PYTHON=/opt/python3/bin/jupyter
export PYSPARK_DRIVER_PYTHON_OPTS=notebook
exec ${SPARK_HOME}/bin/spark-submit pyspark-shell-main --name "PySparkShell" --packages com.amazonaws:aws-java-sdk-pom:1.11.466 --jars=gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar "$@"' >> /usr/bin/jupyspark

chmod +x /usr/bin/jupyspark

# install tmux
apt install tmux apt-transport-https -y

# install sbt
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt update
sudo apt install sbt -y

# Install requirements
conda create -p /opt/python3 python=3.7 -y
conda install -p /opt/python3 pandas jupyter scipy -y
conda install -p /opt/python3 numba -y -c numba
# conda install -p /opt/python3 -c plotly plotly -y
conda install -p /opt/python3 -c conda-forge jupyter_contrib_nbextensions jupyter_nbextensions_configurator pyarrow python-snappy -y
conda install -p /opt/python3 -c r r

echo '#!/bin/bash
/opt/python3/bin/jupyter notebook --allow-root --generate-config -y --ip=127.0.0.1
echo "c.Application.log_level = '"'DEBUG'"'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.ip = '"'0.0.0.0'"'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.port = 8888" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.password = u'"'sha1:5c2fc8ca8fb5:738b32af1bb5b7252c274998b89ce2ad3acdf04e'"'" >> ~/.jupyter/jupyter_notebook_config.py' > /usr/bin/config_jupyter

chmod +x /usr/bin/config_jupyter

/opt/python3/bin/pip install toree
/opt/python3/bin/pip install --upgrade pip
/opt/python3/bin/jupyter toree install --spark_home=/usr/lib/spark --interpreters=Scala,PySpark,SparkR,SQL

conda update -p /opt/python3 --all -y
sudo apt upgrade -y

tmux new-session -d -s "jupyter_session" "config_jupyter && /opt/python3/bin/jupyter notebook"