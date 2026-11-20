from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()   

CHECKPOINT = "/Volumes/bank/ops/checkpoints/fraud"
TARGET = "bank.gold.fraud_alerts"

def build_alerts(src):
    return (src
        .withWatermark("event_time", "5 minutes")
        .groupBy("card_id", F.window("event_time", "10 minutes"))
        .agg(F.count("*").alias("n_tx"), F.sum("amount").alias("total"))
        .where("n_tx >= 5 OR total > 3000")
        .select("card_id",
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "n_tx", "total",
                F.current_timestamp().alias("detected_at")))

def make_upsert(target):
    def upsert(batch_df, batch_id):
        batch_df.createOrReplaceTempView("updates")
        # batch_df.sparkSession, et pas la variable spark globale (Spark Connect / serverless)
        batch_df.sparkSession.sql(f"""
          MERGE INTO {target} t
          USING updates s
          ON t.card_id = s.card_id AND t.window_start = s.window_start
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *""")
    return upsert

def run(output_mode="update", checkpoint=CHECKPOINT, target=TARGET):
    alerts = build_alerts(spark.readStream.table("bank.silver.transactions"))
    return (alerts.writeStream
        .foreachBatch(make_upsert(target))
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start())

if __name__ == "__main__":
    q = run()
    q.awaitTermination()