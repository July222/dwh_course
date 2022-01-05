from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime, timedelta
from dt_staff import MINUS_DT

spark = SparkSession.builder.getOrCreate()

if MINUS_DT > 0:
    processing_dt = str((datetime.utcnow() - timedelta(days=int(MINUS_DT))).date())

    (
        spark.read.format("json")
        .load(f"/data/stg/staff/{processing_dt}")
        .sort(f.col("department_id"))
        .write.mode("overwrite")
        .option("compression", "gzip")
        .parquet(f"/user/dwh202147/data/raw/staff1/date={processing_dt}")
    )
