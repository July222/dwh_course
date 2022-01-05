#!/bin/bash

export PYSPARK_PYTHON=/opt/conda/bin/python
export SPARK_HOME=/usr/lib/spark
cur_dt=`cat dt_delete_object_candidates.py`
cur_dt=${cur_dt:9}

spark-submit --master yarn --deploy-mode cluster --py-files dt_delete_object_candidates.py delete_object_candidates.py

cur_dt=$(($cur_dt-1))
cur_dt="MINUS_DT=${cur_dt}"
echo $cur_dt > dt_delete_object_candidates.py
