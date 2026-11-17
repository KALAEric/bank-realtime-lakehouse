CREATE OR REFRESH STREAMING TABLE bank.bronze.transactions_raw_p
COMMENT 'Bronze du pipeline : copie brute des messages, au format Kafka'
AS SELECT *,
       parse_json(value)    AS payload,
       _metadata.file_path  AS source_file,
       current_timestamp()  AS ingested_at
FROM STREAM read_files('/Volumes/bank/ops/landing/transactions/',
                       format => 'json',
                       schemaHints => 'value STRING');