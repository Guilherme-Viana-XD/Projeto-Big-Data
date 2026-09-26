SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TABLE eventos_raw (
    event_id STRING,
    user_id STRING,
    product_id STRING,
    product_name STRING,
    event_type STRING,
    `timestamp` STRING,
    price DECIMAL(18,2),
    quantity INT,
    category STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'hdfs://namenode:9000/ecommerce/raw',
    'format' = 'json',
    'source.monitor-interval' = '5 s'
);

SELECT
    event_id,
    event_type,
    category,
    `timestamp`
FROM eventos_raw;