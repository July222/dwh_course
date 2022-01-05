from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from dt_delete_object_candidates import MINUS_DT

spark = SparkSession.builder.getOrCreate()

if MINUS_DT > 0:
    processing_dt = str((datetime.utcnow() - timedelta(days=int(MINUS_DT))).date())

    (spark.read.format("csv").option("header", "true").load(f"/data/stg/delete_object_candidates/{processing_dt}")
        .write.mode("overwrite").option("compression", "gzip").parquet(
        f"/user/dwh202147/data/raw/delete_object_candidates/date={processing_dt}")
     )
