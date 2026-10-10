from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as functions


ROOT = Path("/opt/spark/checkpoints/runtime-check")
spark = SparkSession.builder.appName("local-checkpoint-acceptance").config("spark.sql.shuffle.partitions", "2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
output = ROOT / "output"
previous = spark.read.parquet(str(output)).count() if output.exists() else 0
stream = spark.readStream.format("rate-micro-batch").option("rowsPerBatch", 5).load()
stream = stream.select(functions.col("value").alias("event_id"), functions.col("timestamp"))
run = stream.writeStream.format("parquet").option("path", str(output)).option("checkpointLocation", str(ROOT / "checkpoint")).trigger(once=True).start()
run.awaitTermination()
observed = spark.read.parquet(str(output))
count = observed.count()
unique = observed.select("event_id").distinct().count()
if count != previous + 5 or unique != count:
    raise RuntimeError(f"Checkpoint recovery failed: previous={previous}, rows={count}, unique={unique}")
print(json.dumps({"status": "passed", "scope": "Spark runtime/checkpoint smoke only", "previous_rows": previous, "rows": count, "unique_rows": unique}))
spark.stop()