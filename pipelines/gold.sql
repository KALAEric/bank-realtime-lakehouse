CREATE OR REFRESH MATERIALIZED VIEW bank.gold.daily_account_totals
COMMENT 'Nombre et montant des transactions par compte et par jour'
AS SELECT account_id,
          to_date(event_time) AS day,
          count(*)            AS n_tx,
          sum(amount)         AS total_amount
FROM bank.silver.transactions          
GROUP BY account_id, to_date(event_time);