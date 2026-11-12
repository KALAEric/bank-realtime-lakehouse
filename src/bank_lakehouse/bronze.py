def ingest_bronze(spark, source="/Volumes/bank/ops/landing/transactions/",
                  schema_loc="/Volumes/bank/ops/checkpoints/tx_schema",
                  checkpoint="/Volumes/bank/ops/checkpoints/tx_bronze",
                  target="bank.bronze.transactions_raw"):
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source)
        .selectExpr("*", "parse_json(value) AS payload",
                    "_metadata.file_path AS source_file",
                    "current_timestamp() AS ingested_at")
        .writeStream
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true") 
        .trigger(availableNow=True)
        .toTable(target))