-- ===== Bronze : copie brute, protégée contre un full refresh =====
CREATE OR REFRESH STREAMING TABLE bank.bronze.cdc_customers_raw
COMMENT 'CDC Debezium brut, table customers du core banking'
TBLPROPERTIES ('pipelines.reset.allowed' = 'false')      --- prevent full refresh on accidental DELETE
AS SELECT *, _metadata.file_path AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files('/Volumes/bank/ops/landing/cdc/customers/', format => 'json',
  schemaHints => 'op STRING, ts_ms BIGINT,
    before STRUCT<customer_id: STRING, full_name: STRING, email: STRING, address: STRING, country: STRING>,
    after  STRUCT<customer_id: STRING, full_name: STRING, email: STRING, address: STRING, country: STRING>'); --to handle schema evolution from batch 1 (before null) to batch 2 

CREATE OR REFRESH STREAMING TABLE bank.bronze.cdc_accounts_raw
COMMENT 'CDC Debezium brut, table accounts du core banking'
TBLPROPERTIES ('pipelines.reset.allowed' = 'false')
AS SELECT *, _metadata.file_path AS source_file, current_timestamp() AS ingested_at
FROM STREAM read_files('/Volumes/bank/ops/landing/cdc/accounts/', format => 'json',
  schemaHints => 'op STRING, ts_ms BIGINT,
    before STRUCT<account_id: STRING, customer_id: STRING, iban: STRING, status: STRING, balance: DOUBLE>,
    after  STRUCT<account_id: STRING, customer_id: STRING, iban: STRING, status: STRING, balance: DOUBLE>');

-- ===== Vues aplaties =====| keep after rows
CREATE TEMPORARY VIEW cdc_customers_flat AS
SELECT coalesce(after.customer_id, before.customer_id) AS customer_id,  --- pour capturer customer_id_id même  en cas de delete
       after.full_name, after.email, after.address, after.country,
       op, timestamp_millis(ts_ms) AS change_ts
FROM STREAM bank.bronze.cdc_customers_raw;

CREATE TEMPORARY VIEW cdc_accounts_flat AS
SELECT coalesce(after.account_id, before.account_id) AS account_id,
       after.customer_id, after.iban, after.status,
       CAST(after.balance AS DECIMAL(18,2)) AS balance,
       op, timestamp_millis(ts_ms) AS change_ts
FROM STREAM bank.bronze.cdc_accounts_raw;

-- ===== Silver =====
CREATE OR REFRESH STREAMING TABLE bank.silver.customers
COMMENT 'Clients, historisés en SCD 2';

CREATE FLOW customers_cdc AS AUTO CDC INTO bank.silver.customers
FROM STREAM(cdc_customers_flat)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op, change_ts)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (email);

CREATE OR REFRESH STREAMING TABLE bank.silver.accounts
COMMENT 'Comptes, état courant (SCD 1)';

CREATE FLOW accounts_cdc AS AUTO CDC INTO bank.silver.accounts
FROM STREAM(cdc_accounts_flat)
KEYS (account_id)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY change_ts
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 1;