import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// ============================================================
// CONFIGURAÇÃO
// ============================================================

// Data que será processada.
// Pode ser configurada pela variável de ambiente DATA_PROCESSAMENTO.
// Caso não seja informada, utiliza D-1 em UTC.
val dataProcessamento =
  sys.env.getOrElse(
    "DATA_PROCESSAMENTO",
    java.time.LocalDate.now(java.time.ZoneOffset.UTC).minusDays(1).toString
  )

// Caminho dos arquivos históricos no HDFS.
// Também pode ser configurado pela variável CAMINHO_ENTRADA.
val caminhoEntrada =
  sys.env.getOrElse(
    "CAMINHO_ENTRADA",
    "hdfs://namenode:9000/ecommerce/historico"
  )

println("==============================================")
println("ETL - RESUMO DIÁRIO POR CATEGORIA")
println("==============================================")
println(s"Data de processamento: $dataProcessamento")
println(s"Entrada HDFS: $caminhoEntrada")

// Permite sobrescrever somente a partição processada.
spark.conf.set(
  "spark.sql.sources.partitionOverwriteMode",
  "dynamic"
)


// ============================================================
// 1. LEITURA DOS DADOS HISTÓRICOS
// ============================================================

val historico = spark.read
  .option("multiLine", "false")
  .json(caminhoEntrada)

println(s"Total de registros lidos: ${historico.count()}")


// ============================================================
// 2. TIPAGEM DOS CAMPOS
// ============================================================

val tipado = historico
  .withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
  )
  .withColumn(
    "price",
    col("price").cast(DecimalType(12, 2))
  )
  .withColumn(
    "quantity",
    col("quantity").cast(LongType)
  )

println("=== SCHEMA APÓS TIPAGEM ===")
tipado.printSchema()


// ============================================================
// 3. VALIDAÇÃO DOS REGISTROS
// ============================================================

// Um registro é considerado inválido quando:
// - algum campo obrigatório utilizado pelo processamento é nulo;
// - timestamp não pode ser convertido;
// - quantity não é maior que zero;
// - price é negativo.

val invalidos = tipado.filter(
  col("event_id").isNull ||
  col("event_type").isNull ||
  col("category").isNull ||
  col("timestamp").isNull ||
  col("price").isNull ||
  col("quantity").isNull ||
  col("quantity") <= 0 ||
  col("price") < 0
)

val qtdInvalidos = invalidos.count()

println(s"Registros inválidos encontrados: $qtdInvalidos")


// Mantém somente os registros válidos.

val validos = tipado.filter(
  col("event_id").isNotNull &&
  col("event_type").isNotNull &&
  col("category").isNotNull &&
  col("timestamp").isNotNull &&
  col("price").isNotNull &&
  col("quantity").isNotNull &&
  col("quantity") > 0 &&
  col("price") >= 0
)

println(s"Registros válidos: ${validos.count()}")


// ============================================================
// 4. DEDUPLICAÇÃO
// ============================================================

// event_id é utilizado como identificador único do evento.

val semDuplicatas =
  validos.dropDuplicates("event_id")

println(
  s"Registros após deduplicação: ${semDuplicatas.count()}"
)


// ============================================================
// 5. FILTRO DA DATA DE PROCESSAMENTO
// ============================================================

// A data é obtida pelo timestamp do evento.
// Portanto, o processamento NÃO depende do nome do arquivo.

val eventosData = semDuplicatas.filter(
  to_date(col("timestamp")) ===
    lit(dataProcessamento).cast(DateType)
)

println(
  s"Registros da data $dataProcessamento: ${eventosData.count()}"
)


// ============================================================
// 6. AGREGAÇÃO DE PRODUCT_VIEW
// ============================================================

val viewsPorCategoria = eventosData
  .filter(
    col("event_type") === "product_view"
  )
  .withColumn(
    "data",
    to_date(col("timestamp"))
  )
  .groupBy(
    "data",
    "category"
  )
  .agg(
    count("*").alias("qtd_product_view")
  )


// ============================================================
// 7. AGREGAÇÃO DE PURCHASE
// ============================================================

val comprasPorCategoria = eventosData
  .filter(
    col("event_type") === "purchase"
  )
  .withColumn(
    "data",
    to_date(col("timestamp"))
  )
  .groupBy(
    "data",
    "category"
  )
  .agg(
    count("*")
      .alias("qtd_purchase"),

    sum(col("quantity"))
      .alias("qtd_itens_purchase"),

    sum(
      col("price") * col("quantity")
    )
      .cast(DecimalType(18, 2))
      .alias("valor_total_purchase")
  )


// ============================================================
// 8. JUNÇÃO DOS INDICADORES
// ============================================================

// FULL OUTER garante que categorias que possuam somente
// visualizações ou somente compras não sejam descartadas.

val resumo = viewsPorCategoria.join(
  comprasPorCategoria,
  Seq(
    "data",
    "category"
  ),
  "full_outer"
)


// ============================================================
// 9. TRATAMENTO DOS VALORES AUSENTES
// ============================================================

val resumoComZeros = resumo.na.fill(
  0L,
  Seq(
    "qtd_product_view",
    "qtd_purchase",
    "qtd_itens_purchase"
  )
)

val resumoFinal = resumoComZeros.withColumn(
  "valor_total_purchase",
  coalesce(
    col("valor_total_purchase"),
    lit(0).cast(DecimalType(18, 2))
  ).cast(DecimalType(18, 2))
)


// ============================================================
// 10. EXIBIÇÃO DO RESULTADO
// ============================================================

println(
  s"=== RESUMO DIÁRIO - $dataProcessamento ==="
)

resumoFinal
  .orderBy("category")
  .show(false)


// ============================================================
// 11. BANCO E TABELA HIVE
// ============================================================

spark.sql(
  "CREATE DATABASE IF NOT EXISTS ecommerce"
)

spark.sql("""
  CREATE TABLE IF NOT EXISTS ecommerce.resumo_diario_categoria (
    category STRING,
    qtd_product_view BIGINT,
    qtd_purchase BIGINT,
    qtd_itens_purchase BIGINT,
    valor_total_purchase DECIMAL(18,2)
  )
  USING PARQUET
  PARTITIONED BY (data DATE)
""")


// ============================================================
// 12. PREPARAÇÃO PARA GRAVAÇÃO
// ============================================================

// A coluna de partição deve permanecer por último,
// seguindo a estrutura da tabela Hive.

val paraHive = resumoFinal.select(
  "category",
  "qtd_product_view",
  "qtd_purchase",
  "qtd_itens_purchase",
  "valor_total_purchase",
  "data"
)


// ============================================================
// 13. GRAVAÇÃO NO HIVE
// ============================================================

// partitionOverwriteMode=dynamic faz com que apenas
// a partição presente neste DataFrame seja sobrescrita.

paraHive.write
  .mode("overwrite")
  .insertInto(
    "ecommerce.resumo_diario_categoria"
  )

println(
  s"Partição $dataProcessamento gravada com sucesso no Hive."
)


// ============================================================
// 14. VERIFICAÇÃO DAS PARTIÇÕES
// ============================================================

println("=== PARTIÇÕES DA TABELA HIVE ===")

spark.sql(
  "SHOW PARTITIONS ecommerce.resumo_diario_categoria"
).show(false)


// ============================================================
// 15. CONSULTA DE VALIDAÇÃO
// ============================================================

println(
  s"=== DADOS GRAVADOS NO HIVE PARA $dataProcessamento ==="
)

spark.sql(s"""
  SELECT
    data,
    category,
    qtd_product_view,
    qtd_purchase,
    qtd_itens_purchase,
    valor_total_purchase
  FROM ecommerce.resumo_diario_categoria
  WHERE data = DATE '$dataProcessamento'
  ORDER BY category
""").show(false)


// ============================================================
// 16. PLANO DE EXECUÇÃO / SHUFFLE
// ============================================================

// No plano físico, operações "Exchange" demonstram
// redistribuição de dados (shuffle).
//
// Neste processamento, shuffle pode ocorrer principalmente em:
// - dropDuplicates(event_id)
// - groupBy(data, category)
// - full_outer join por data e category

println("=== PLANO DE EXECUÇÃO / SHUFFLE ===")

resumoFinal.explain(true)


// ============================================================
// FINALIZAÇÃO
// ============================================================

println("==============================================")
println("ETL FINALIZADO COM SUCESSO")
println("==============================================")
