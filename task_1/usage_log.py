from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime, timedelta
from dt_usage_log import MINUS_DT

spark = SparkSession.builder.getOrCreate()

if MINUS_DT > 0:
    processing_dt = str((datetime.utcnow() - timedelta(days=int(MINUS_DT))).date())

    (
        spark.read.format("xml").option("rootTag", "usages")
        .option("rowTag", "usage").load(f"/data/stg/usage_log/{processing_dt}").filter("ts!=-1")
        .withColumn("ts", f.from_unixtime(f.col("ts"))).sort(f.col("staff_login"))
        .write.mode("overwrite").option("compression", "gzip").parquet(
            f"/user/dwh202147/data/raw/usage_log1/date={processing_dt}")
    )
