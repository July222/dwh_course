#!/bin/bash

export PYSPARK_PYTHON=/opt/conda/bin/python
export SPARK_HOME=/usr/lib/spark
cur_dt=`cat dt_usage_log.py`
cur_dt=${cur_dt:9}

spark-submit --master yarn --deploy-mode cluster --packages com.databricks:spark-xml_2.11:0.4.1 --py-files dt_usage_log.py usage_log.py

cur_dt=$(($cur_dt-1))
cur_dt="MINUS_DT=${cur_dt}"
echo $cur_dt > dt_usage_log.py
