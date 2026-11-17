-- Vue interne au pipeline
CREATE TEMPORARY VIEW tx_typed AS
SELECT payload:txn_id::string            AS txn_id,
       payload:card_id::string           AS card_id,
       payload:pan::string               AS pan,
       payload:account_id::string        AS account_id,
       payload:amount::decimal(18,2)     AS amount,
       payload:currency::string          AS currency,
       payload:merchant.name::string     AS merchant_name,
       payload:merchant.mcc::int         AS merchant_mcc,
       payload:country::string           AS country,
       payload:channel::string           AS channel,
       payload:event_time::timestamp     AS event_time,
       payload:device_id::string         AS device_id,
       source_file, ingested_at
FROM STREAM bank.bronze.transactions_raw_p;

CREATE OR REFRESH STREAMING TABLE bank.silver.transactions_valid (
  CONSTRAINT txn_id_present  EXPECT (txn_id IS NOT NULL)          ON VIOLATION DROP ROW,
  CONSTRAINT amount_positive EXPECT (coalesce(amount > 0, false)) ON VIOLATION DROP ROW,
  CONSTRAINT known_currency  EXPECT (currency IN ('EUR', 'USD', 'GBP'))   -- warn 
)
COMMENT 'Transactions typées qui passent les règles de qualité'
AS SELECT * FROM STREAM tx_typed;

CREATE OR REFRESH STREAMING TABLE bank.silver.transactions_quarantine
COMMENT 'Lignes rejetées par les règles DROP, avec leur motif'
AS SELECT *,
       CASE WHEN txn_id IS NULL THEN 'txn_id_null' ELSE 'amount_invalid' END AS reject_reason
FROM STREAM tx_typed
WHERE NOT (txn_id IS NOT NULL AND coalesce(amount > 0, false));