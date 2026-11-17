from pyspark import pipelines as dp

@dp.table(name="bank.silver.transactions",
          comment="Transactions valides, dédupliquées sur txn_id (renvois Kafka)")
def transactions():
    return (spark.readStream.table("bank.silver.transactions_valid")
            .withWatermark("event_time", "2 days")
            .dropDuplicatesWithinWatermark(["txn_id"])); 