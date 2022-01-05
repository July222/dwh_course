# dwh_course

Uploading 3 datasets to HDFS using Spark.
Bash scripts runs every 5 minutes (schedule is set by crontab).
Files dt_*.py contain variable MINUS_DT which contains the number of how many previous days we should upload.
After bash script execution we decrease its' value by 1. We stop spark jobs execution when its' value becomes negative.