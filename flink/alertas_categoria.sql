SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';

-- Limite do alerta.
-- Alterar somente este valor para configurar o MVP.
CREATE TEMPORARY VIEW configuracao_alerta AS
SELECT CAST(2 AS BIGINT) AS limite_alerta;

CREATE TABLE eventos_raw (
    event_id STRING,
    user_id STRING,
    product_id STRING,
    product_name STRING,
    event_type STRING,
    `timestamp` TIMESTAMP_LTZ(3),
    price DECIMAL(18,2),
    quantity INT,
    category STRING,

    WATERMARK FOR `timestamp`
        AS `timestamp` - INTERVAL '60' SECOND
) WITH (
    'connector' = 'filesystem',
    'path' = 'hdfs://namenode:9000/ecommerce/raw',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'source.monitor-interval' = '5 s'
);

CREATE TABLE alertas_hbase (
    rowkey STRING,

    info ROW<
        categoria STRING,
        inicio_janela STRING,
        fim_janela STRING,
        visualizacoes STRING,
        limite_alerta STRING
    >,

    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase-2.2',
    'table-name' = 'alertas_categoria',
    'zookeeper.quorum' = 'hbase:2181'
);

INSERT INTO alertas_hbase
SELECT
    CONCAT(
        resultado.category,
        '|',
        CAST(resultado.window_start AS STRING),
        '|',
        CAST(resultado.window_end AS STRING)
    ) AS rowkey,

    ROW(
        resultado.category,
        CAST(resultado.window_start AS STRING),
        CAST(resultado.window_end AS STRING),
        CAST(resultado.visualizacoes AS STRING),
        CAST(config.limite_alerta AS STRING)
    )
FROM (
    SELECT
        window_start,
        window_end,
        category,
        COUNT(*) AS visualizacoes
    FROM TABLE(
        HOP(
            TABLE eventos_raw,
            DESCRIPTOR(`timestamp`),
            INTERVAL '10' SECOND,
            INTERVAL '60' SECOND
        )
    )
    WHERE event_type = 'product_view'
    GROUP BY
        window_start,
        window_end,
        category
) resultado
CROSS JOIN configuracao_alerta config
WHERE resultado.visualizacoes >= config.limite_alerta;