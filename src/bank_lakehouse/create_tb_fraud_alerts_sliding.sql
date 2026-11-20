CREATE TABLE IF NOT EXISTS bank.gold.fraud_alerts_sliding 
(card_id STRING, 
 window_start TIMESTAMP, 
 window_end TIMESTAMP,
 n_tx BIGINT, 
 total DECIMAL(28,2), 
detected_at TIMESTAMP)
COMMENT 'Alertes de fraude (vélocité et montant), écrites par streaming/fraud_detection.py en append';