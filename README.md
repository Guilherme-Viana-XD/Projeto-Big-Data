# Gerador de Eventos - MVP Big Data

Gerador de eventos sintéticos para o projeto de Big Data.

O gerador possui dois modos:

* geração contínua de eventos para processamento em streaming;
* geração de dados históricos para processamento em batch.

## Estrutura

```
gerador/
└── gerador.py

dados/
├── eventos.json
└── historico_*.json

scripts/
└── validar_eventos.py
```

## Gerador

O gerador produz eventos sintéticos e independentes.

Os eventos não possuem sessões, funil de compra ou relação entre pedidos. Cada evento é gerado de forma aleatória e independente dos demais.

Os tipos de evento utilizados são:

* `page_view`
* `product_view`
* `add_to_cart`
* `remove_from_cart`
* `purchase`
* `payment_approved`
* `shipping`
* `delivered`

Cada produto possui uma categoria fixa:

* `prod_001` - Notebook → `informatica`
* `prod_002` - Smartphone → `telefonia`
* `prod_003` - Fone Bluetooth → `audio`
* `prod_004` - Teclado Mecânico → `perifericos`
* `prod_005` - Mouse Gamer → `perifericos`
* `prod_006` - Monitor → `informatica`

## Modo contínuo

Para iniciar a geração contínua de eventos:

```
python gerador/gerador.py
```

Os eventos são adicionados ao arquivo:

```
dados/eventos.json
```

Cada evento é escrito em uma linha no formato JSON.

Para interromper o gerador:

```
Ctrl + C
```

O gerador não utiliza o atraso do timestamp como tempo de espera. Aproximadamente 90% dos eventos utilizam o horário atual e aproximadamente 10% recebem um atraso aleatório entre 5 e 45 segundos.

Isso permite simular eventos que podem chegar fora de ordem temporal.

## Geração de histórico

Para gerar um arquivo histórico:

```
python gerador/gerador.py --historico 2026-09-21 --total 10000 --saida dados/historico_2026-09-21.json
```

O comando gera exatamente a quantidade de eventos informada.

Os timestamps pertencem ao dia informado e são armazenados em UTC.

Caso o arquivo de saída já exista, ele é substituído.

## Formato dos eventos

Cada evento possui os seguintes campos:

* `event_id`
* `user_id`
* `product_id`
* `product_name`
* `event_type`
* `timestamp`
* `price`
* `quantity`
* `category`

Exemplo:

```
{
  "event_id": "abc123",
  "user_id": "user_001",
  "product_id": "prod_001",
  "product_name": "Notebook",
  "event_type": "product_view",
  "timestamp": "2026-09-22T18:30:15.250Z",
  "price": 3500.0,
  "quantity": 1,
  "category": "informatica"
}
```

O campo `timestamp` utiliza UTC com precisão de milissegundos e o formato termina com `Z`.

## Validação

Para validar um arquivo de eventos:

```
python scripts/validar_eventos.py dados/historico_2026-09-21.json
```

A validação verifica:

* quantidade de eventos;
* linhas inválidas;
* IDs duplicados;
* campos obrigatórios;
* tipos dos campos;
* timestamps;
* categorias dos produtos;
* tipos de eventos;
* timestamps fora de ordem.

## Compatibilidade com Flume

O arquivo:

```
dados/eventos.json
```

continua sendo utilizado como fonte para o processamento de eventos em streaming.

A configuração do Flume permanece compatível com o gerador.

## Objetivo do MVP

O gerador fornece dados para duas formas de processamento:

* **Streaming:** geração contínua de eventos para processamento em tempo real;
* **Batch:** geração de arquivos históricos para processamento em lote.

Os dados são sintéticos e têm finalidade acadêmica para testes do pipeline de Big Data.

---

# Processamento Batch com Spark e Hive

O processamento batch utiliza Apache Spark para ler os eventos históricos armazenados no HDFS, realizar validação, deduplicação e agregações e persistir o resultado no Apache Hive.

## Arquitetura Batch

```text
Gerador histórico
      |
      v
dados/historico_*.json
      |
      v
     HDFS
/ecommerce/historico
      |
      v
 Apache Spark
      |
      v
Validação e agregação
      |
      v
 Apache Hive
ecommerce.resumo_diario_categoria
```

## Estrutura

Os principais arquivos utilizados no processamento batch são:

```text
docker-compose.batch.yml

hive/
└── hive-site.xml

spark/
└── etl_resumo_diario.scala
```

## Serviços

O ambiente batch utiliza:

- Apache Spark;
- Apache Hive Metastore;
- PostgreSQL como banco do Hive Metastore;
- HDFS como armazenamento dos arquivos históricos e do warehouse Hive.

O Spark e o Hive utilizam a mesma rede Docker do ambiente Hadoop.

## Entrada dos dados

Os arquivos históricos são armazenados no HDFS em:

```text
/ecommerce/historico
```

O job Spark lê esse diretório e seleciona os registros pela data presente no campo `timestamp`.

O processamento não depende do nome do arquivo.

## Data de processamento

A data pode ser configurada pela variável de ambiente:

```text
DATA_PROCESSAMENTO
```

Quando ela não é informada, o job utiliza automaticamente D-1 em UTC.

O caminho de entrada também pode ser configurado através da variável:

```text
CAMINHO_ENTRADA
```

O valor padrão é:

```text
hdfs://namenode:9000/ecommerce/historico
```

## Tipagem dos dados

Durante o processamento, os campos utilizados pelos indicadores são convertidos para tipos apropriados.

Principais conversões:

```text
timestamp -> TIMESTAMP
price     -> DECIMAL(12,2)
quantity  -> LONG
```

Os valores monetários calculados no resultado final são armazenados como:

```text
DECIMAL(18,2)
```

## Regras de validação

Antes das agregações, o Spark valida os registros utilizados no processamento.

São considerados inválidos registros que possuam:

- `event_id` nulo;
- `event_type` nulo;
- `category` nulo;
- `timestamp` inválido ou nulo;
- `price` inválido ou nulo;
- `quantity` inválido ou nulo;
- `quantity` menor ou igual a zero;
- `price` negativo.

A quantidade de registros inválidos é contabilizada durante a execução.

Os registros inválidos não participam das agregações analíticas.

## Deduplicação

Depois da validação, os eventos são deduplicados utilizando:

```text
event_id
```

O processamento utiliza:

```scala
dropDuplicates("event_id")
```

Isso evita que o mesmo evento seja contabilizado mais de uma vez.

## Filtro da data

A seleção do dia processado é feita utilizando o campo:

```text
timestamp
```

A data é extraída do próprio evento.

Exemplo conceitual:

```scala
to_date(col("timestamp"))
```

Dessa forma, o processamento não depende de nomes como:

```text
historico_2026-09-25.json
```

Mesmo que diferentes datas estejam presentes no mesmo diretório, somente os eventos correspondentes à data informada em `DATA_PROCESSAMENTO` são utilizados.

## Indicadores

Os dados são agregados por:

```text
data + category
```

### Product View

Para eventos:

```text
product_view
```

é calculado:

```text
qtd_product_view
```

Esse indicador representa a quantidade de visualizações de produtos por categoria no dia processado.

### Purchase

Para eventos:

```text
purchase
```

são calculados:

```text
qtd_purchase
qtd_itens_purchase
valor_total_purchase
```

Onde:

- `qtd_purchase`: quantidade de eventos de compra;
- `qtd_itens_purchase`: soma das quantidades compradas;
- `valor_total_purchase`: soma de `price * quantity`.

O cálculo financeiro utiliza:

```text
valor_total_purchase = SUM(price * quantity)
```

O resultado monetário utiliza:

```text
DECIMAL(18,2)
```

## FULL OUTER JOIN

As agregações de `product_view` e `purchase` são feitas separadamente.

Depois, os dois resultados são combinados utilizando:

```text
FULL OUTER JOIN
```

A chave da junção é:

```text
data + category
```

Essa abordagem garante que uma categoria continue aparecendo quando possuir:

- somente visualizações;
- somente compras;
- visualizações e compras.

As métricas ausentes são preenchidas com zero.

Exemplo:

```text
Categoria com views e nenhuma compra:

qtd_product_view = 2
qtd_purchase = 0
qtd_itens_purchase = 0
valor_total_purchase = 0.00
```

## Resultado final

O DataFrame final possui:

```text
data
category
qtd_product_view
qtd_purchase
qtd_itens_purchase
valor_total_purchase
```

Antes da gravação no Hive, a ordem é ajustada para manter a coluna de partição no final:

```text
category
qtd_product_view
qtd_purchase
qtd_itens_purchase
valor_total_purchase
data
```

## Banco Hive

O banco utilizado é:

```text
ecommerce
```

Ele é criado caso ainda não exista:

```sql
CREATE DATABASE IF NOT EXISTS ecommerce
```

## Tabela Hive

Os resultados são armazenados em:

```text
ecommerce.resumo_diario_categoria
```

A estrutura da tabela é:

```text
category               STRING
qtd_product_view       BIGINT
qtd_purchase           BIGINT
qtd_itens_purchase     BIGINT
valor_total_purchase   DECIMAL(18,2)
data                    DATE
```

A tabela utiliza:

```text
PARQUET
```

e é particionada por:

```text
data
```

## Particionamento

Cada data processada corresponde a uma partição da tabela.

Exemplo:

```text
data=2026-09-24
data=2026-09-25
```

As partições podem ser consultadas com:

```scala
spark.sql(
  "SHOW PARTITIONS ecommerce.resumo_diario_categoria"
).show(false)
```

## Reprocessamento e idempotência

O Spark utiliza:

```text
spark.sql.sources.partitionOverwriteMode = dynamic
```

Isso habilita sobrescrita dinâmica das partições.

Ao processar novamente uma data, somente a partição correspondente é substituída.

Por exemplo, ao reprocessar:

```text
2026-09-25
```

a partição:

```text
data=2026-09-25
```

é sobrescrita.

Uma partição existente de outra data, como:

```text
data=2026-09-24
```

é preservada.

Dessa forma:

- executar novamente o mesmo dia não duplica os indicadores;
- o processamento é idempotente para a data reprocessada;
- partições de outras datas permanecem preservadas.

## Ambiente Docker Batch

O ambiente batch é definido no arquivo:

```text
docker-compose.batch.yml
```

Os principais serviços são:

```text
metastore-db
hive-metastore
spark
```

O PostgreSQL armazena os metadados do Hive Metastore.

O Spark utiliza o arquivo:

```text
hive/hive-site.xml
```

para acessar o Hive Metastore e o warehouse localizado no HDFS.

## Iniciando o ambiente batch

O Hadoop/HDFS deve estar em execução antes dos componentes batch.

Para iniciar o ambiente:

```powershell
docker compose -f .\docker-compose.batch.yml up -d
```

Para verificar os containers:

```powershell
docker compose -f .\docker-compose.batch.yml ps
```

## Diretório histórico no HDFS

Os dados históricos utilizados pelo Spark ficam em:

```text
/ecommerce/historico
```

Exemplo de arquivo:

```text
/ecommerce/historico/historico_2026-09-25.json
```

Para listar:

```powershell
docker exec projeto_bigdata_namenode hdfs dfs -ls /ecommerce/historico
```

## Executando o ETL

O script principal é:

```text
spark/etl_resumo_diario.scala
```

Exemplo para processar o dia:

```text
2026-09-25
```

Execute:

```powershell
docker exec -e DATA_PROCESSAMENTO=2026-09-25 -e CAMINHO_ENTRADA=hdfs://namenode:9000/ecommerce/historico -it projeto_bigdata_spark /opt/spark/bin/spark-shell --master "local[2]" --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 -i /workspace/etl_resumo_diario.scala
```

Durante a execução, o job apresenta:

- data de processamento;
- caminho de entrada;
- quantidade de registros lidos;
- schema após tipagem;
- quantidade de registros inválidos;
- quantidade de registros válidos;
- quantidade após deduplicação;
- quantidade de eventos da data escolhida;
- resumo diário por categoria;
- partições existentes no Hive;
- resultado armazenado;
- plano de execução Spark.

## Resultado de validação

O processamento do histórico de:

```text
2026-09-25
```

produziu:

| Categoria | Product Views | Purchases | Itens Comprados | Valor Total |
|---|---:|---:|---:|---:|
| audio | 1 | 2 | 6 | 1199.40 |
| informatica | 6 | 5 | 9 | 20000.00 |
| perifericos | 7 | 3 | 6 | 1250.00 |
| telefonia | 1 | 3 | 5 | 11000.00 |

## Consultando os dados no Hive através do Spark

Dentro do Spark Shell:

```scala
spark.sql("""
SELECT *
FROM ecommerce.resumo_diario_categoria
ORDER BY data, category
""").show(false)
```

Para consultar apenas uma data:

```scala
spark.sql("""
SELECT *
FROM ecommerce.resumo_diario_categoria
WHERE data = DATE '2026-09-25'
ORDER BY category
""").show(false)
```

Resultado validado:

```text
+-----------+----------------+------------+------------------+--------------------+----------+
|category   |qtd_product_view|qtd_purchase|qtd_itens_purchase|valor_total_purchase|data      |
+-----------+----------------+------------+------------------+--------------------+----------+
|audio      |1               |2           |6                 |1199.40             |2026-09-25|
|informatica|6               |5           |9                 |20000.00            |2026-09-25|
|perifericos|7               |3           |6                 |1250.00             |2026-09-25|
|telefonia  |1               |3           |5                 |11000.00            |2026-09-25|
+-----------+----------------+------------+------------------+--------------------+----------+
```

## Persistência do Hive

A tabela foi validada também após encerrar uma sessão do Spark e iniciar uma nova.

Os dados continuaram disponíveis em:

```text
ecommerce.resumo_diario_categoria
```

Isso confirma que as informações não existem somente em memória durante a execução do Spark.

## Teste de idempotência

O mesmo processamento foi executado mais de uma vez para a mesma data.

Após o reprocessamento, os valores permaneceram iguais e não houve duplicação.

Também foi validado o processamento de uma segunda data.

As partições existentes foram:

```text
data=2026-09-24
data=2026-09-25
```

Ao reprocessar uma das datas, a outra permaneceu disponível.

## Shuffle

Algumas operações utilizadas pelo processamento exigem redistribuição dos dados entre as partições do Spark.

As principais são:

```scala
dropDuplicates("event_id")
```

```scala
groupBy("data", "category")
```

e o:

```text
FULL OUTER JOIN
```

Essas operações precisam reunir registros que possuem as mesmas chaves em partições adequadas para realizar deduplicação, agregações e junções.

## Visualizando o plano de execução

O plano lógico e físico pode ser visualizado com:

```scala
resumoFinal.explain(true)
```

No plano físico, operações identificadas como:

```text
Exchange
```

demonstram redistribuição dos dados entre partições, caracterizando operações de shuffle.

Durante os testes também foi observado o uso de operações como:

```text
Exchange hashpartitioning
SortMergeJoin
```

na execução das agregações e do `FULL OUTER JOIN`.

## Fluxo completo do processamento batch

```text
Arquivo histórico JSON
        |
        v
       HDFS
/ecommerce/historico
        |
        v
Spark lê os eventos
        |
        v
Conversão dos tipos
        |
        v
Validação
        |
        v
Remoção dos inválidos
        |
        v
Deduplicação por event_id
        |
        v
Filtro pela data do timestamp
        |
        +----------------------+
        |                      |
        v                      v
 product_view               purchase
        |                      |
        v                      v
    groupBy                 groupBy
 data + category         data + category
        |                      |
        +----------+-----------+
                   |
                   v
            FULL OUTER JOIN
                   |
                   v
       Preenchimento com zeros
                   |
                   v
         Resumo diário final
                   |
                   v
               Hive
                   |
                   v
ecommerce.resumo_diario_categoria
       particionada por data
```

## Objetivo do processamento batch

A etapa Spark/Hive permite transformar os eventos históricos brutos armazenados no HDFS em indicadores analíticos consolidados por dia e categoria.

O processamento garante:

- validação dos dados;
- deduplicação dos eventos;
- seleção por timestamp;
- agregações por categoria;
- preservação de categorias com apenas views ou compras;
- valores monetários utilizando Decimal;
- persistência no Hive;
- particionamento por data;
- reprocessamento idempotente;
- preservação das demais partições;
- demonstração de operações de shuffle no Spark.
