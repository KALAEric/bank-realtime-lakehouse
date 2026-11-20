from pyspark import pipelines as dp
from pyspark.sql import functions as F


dp.create_streaming_table(
    name="bank.silver.customers_current",
    comment="Clients, état courant (SCD 1), même source que bank.silver.customers")

dp.create_auto_cdc_flow(
    target="bank.silver.customers_current",
    source="cdc_customers_flat",
    keys=["customer_id"],
    sequence_by=F.col("change_ts"),
    apply_as_deletes=F.expr("op = 'd'"),
    except_column_list=["op", "change_ts"],
    stored_as_scd_type=1,
)