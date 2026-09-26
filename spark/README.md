# Processamento D-1 com Spark e Hive

Este módulo implementa o processamento batch dos eventos históricos do projeto de Big Data utilizando Apache Spark, HDFS e Hive.

O objetivo é processar os eventos referentes a uma data específica — por padrão, o dia anterior em UTC — gerar indicadores diários por categoria e persistir o resultado em uma tabela Hive consultável e particionada por data.

---

## Objetivo

O processamento realiza as seguintes etapas:

1. Leitura dos eventos históricos armazenados no HDFS.
2. Conversão e validação dos tipos utilizados pelo ETL.
3. Descarte de registros inválidos.
4. Remoção de duplicatas por `event_id`.
5. Filtro dos registros pela data do campo `timestamp`.
6. Separação entre visualizações e compras.
7. Agregação dos indicadores por data e categoria.
8. Junção das agregações utilizando `FULL OUTER JOIN`.
9. Persistência dos resultados no Hive.
10. Reprocessamento seguro de uma data sem duplicar resultados anteriores.

---

## Tecnologias utilizadas

- Apache Spark 3.5.7
- Scala 2.12
- Java 11
- Apache Hive
- PostgreSQL para persistência do Hive Metastore
- Hadoop / HDFS
- Docker
- Docker Compose

O processamento Spark é executado em modo local:

```text
local[2]
```

Essa configuração é suficiente para o MVP e evita a necessidade de adicionar um cluster Spark distribuído ao ambiente.

---

## Estrutura

Principais arquivos utilizados:

```text
spark/
├── etl_resumo_diario.scala
├── test-data/
│   └── teste_controlado_2026-09-24.json
└── README.md

hive/
└── Dockerfile

docker-compose.batch.yml
```

O arquivo `etl_resumo_diario.scala` contém a implementação principal do processamento.

O diretório `test-data` contém uma pequena amostra controlada utilizada para validar o comportamento do ETL.

---

## Subindo o ambiente

A partir da raiz do projeto:

```powershell
docker compose -f docker-compose.yml -f docker-compose.batch.yml up -d --build
```

Para conferir os containers:

```powershell
docker compose -f docker-compose.yml -f docker-compose.batch.yml ps
```

---

## Hive Metastore

O projeto utiliza um Hive Metastore persistente apoiado por PostgreSQL.

A imagem do Hive é construída a partir de:

```text
hive/Dockerfile
```

Ela utiliza como base:

```text
bde2020/hive:2.3.2-postgresql-metastore
```

e substitui o driver PostgreSQL antigo por uma versão atualizada do JDBC.

Isso permite que o Spark e as consultas SQL utilizem o mesmo metastore.

---

## Dados históricos no HDFS

Os dados históricos utilizados pelo processamento batch ficam em:

```text
/ecommerce/historico
```

Esse diretório é separado do fluxo utilizado pelo processamento em tempo real.

Para visualizar os arquivos:

```powershell
docker compose -f docker-compose.yml -f docker-compose.batch.yml exec namenode hdfs dfs -ls /ecommerce/historico
```

Exemplo:

```text
/ecommerce/historico/teste_batch_2026-09-25.json
/ecommerce/historico/teste_controlado_2026-09-24.json
```

---

# Carregando um arquivo histórico no HDFS

Primeiro copie o arquivo local para o container do NameNode:

```powershell
docker compose -f docker-compose.yml -f docker-compose.batch.yml cp .\spark\test-data\teste_controlado_2026-09-24.json namenode:/tmp/teste_controlado_2026-09-24.json
```

Garanta que o diretório histórico exista:

```powershell
docker compose -f docker-compose.yml -f docker-compose.batch.yml exec namenode hdfs dfs -mkdir -p /ecommerce/historico
```

Envie o arquivo ao HDFS:

```powershell
docker compose -f docker-compose.yml -f docker-compose.batch.yml exec namenode hdfs dfs -put /tmp/teste_controlado_2026-09-24.json /ecommerce/historico/teste_controlado_2026-09-24.json
```

Confirme a carga:

```powershell
docker compose -f docker-compose.yml -f docker-compose.batch.yml exec namenode hdfs dfs -ls /ecommerce/historico
```

---

# Execução do ETL

## Execução com data informada

A data pode ser definida através da variável:

```text
DATA_PROCESSAMENTO
```

O caminho de entrada pode ser definido através de:

```text
CAMINHO_ENTRADA
```

Exemplo para processar `2026-09-25`:

```powershell
docker compose -f docker-compose.yml -f docker-compose.batch.yml exec -e DATA_PROCESSAMENTO=2026-09-25 -e CAMINHO_ENTRADA=hdfs://namenode:9000/ecommerce/historico/teste_batch_2026-09-25.json spark /opt/spark/bin/spark-shell --master "local[2]" --conf spark.sql.catalogImplementation=hive --conf spark.sql.session.timeZone=UTC --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 -i /workspace/etl_resumo_diario.scala
```

---

## Execução automática D-1

Se `DATA_PROCESSAMENTO` não for informada, o ETL utiliza automaticamente o dia anterior em UTC.

Exemplo:

```powershell
docker compose -f docker-compose.yml -f docker-compose.batch.yml exec -e CAMINHO_ENTRADA=hdfs://namenode:9000/ecommerce/historico/teste_batch_2026-09-25.json spark /opt/spark/bin/spark-shell --master "local[2]" --conf spark.sql.catalogImplementation=hive --conf spark.sql.session.timeZone=UTC --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 -i /workspace/etl_resumo_diario.scala
```

Em uma execução realizada em `2026-09-26`, o processamento selecionou automaticamente:

```text
Data de processamento: 2026-09-25
```

O timezone utilizado pelo Spark é explicitamente configurado como UTC.

---

# Validação dos dados

Antes da agregação são aplicadas validações aos campos necessários ao processamento.

Entre os campos utilizados estão:

```text
event_id
event_type
category
timestamp
price
quantity
```

São descartados registros que não possuem os campos obrigatórios válidos.

Também são rejeitados registros com:

```text
quantity <= 0
```

ou preço inválido.

---

## Tipos utilizados

Após a tipagem, os principais campos possuem os seguintes tipos:

```text
category   string
event_id   string
event_type string
price      decimal(12,2)
quantity   long
timestamp  timestamp
```

Os cálculos monetários utilizam `Decimal`, evitando operações financeiras baseadas diretamente em ponto flutuante.

O resultado monetário armazenado utiliza precisão decimal.

---

# Deduplicação

Os eventos duplicados são removidos utilizando:

```text
event_id
```

Assim, duas ocorrências do mesmo evento não aumentam artificialmente os indicadores.

---

# Filtro por data

A seleção da data é feita utilizando o campo:

```text
timestamp
```

do próprio evento.

O processamento não depende da data do arquivo nem do nome do arquivo armazenado no HDFS.

---

# Indicadores produzidos

Os resultados são agrupados por:

```text
data
category
```

São produzidos quatro indicadores.

## Quantidade de visualizações

Quantidade de eventos:

```text
product_view
```

Campo resultante:

```text
qtd_product_view
```

---

## Quantidade de compras

Quantidade de eventos:

```text
purchase
```

Campo resultante:

```text
qtd_purchase
```

---

## Unidades compradas

Soma de:

```text
quantity
```

dos eventos de compra.

Campo resultante:

```text
qtd_itens_purchase
```

---

## Valor total das compras

Calculado através de:

```text
price × quantity
```

para os eventos de compra.

Campo resultante:

```text
valor_total_purchase
```

---

# Junção entre visualizações e compras

As visualizações e compras são agregadas separadamente.

Depois é realizado um:

```text
FULL OUTER JOIN
```

utilizando:

```text
data
category
```

Isso permite preservar categorias que aparecem somente em visualizações ou somente em compras.

Não é calculada taxa de conversão, pois os eventos são independentes.

---

# Shuffle

O plano físico do Spark apresenta operações como:

```text
Exchange hashpartitioning(...)
```

antes das agregações.

Isso demonstra a ocorrência de shuffle.

O shuffle é necessário porque, para executar uma agregação por:

```text
data
category
```

o Spark precisa redistribuir entre as partições todos os registros que possuem a mesma chave.

Dessa forma, registros da mesma data e categoria conseguem ser processados juntos durante a agregação.

O plano também utiliza:

```text
SortMergeJoin
```

para realizar o `FULL OUTER JOIN` entre as agregações de visualizações e compras.

---

# Persistência no Hive

O resultado é armazenado no banco:

```text
ecommerce
```

e na tabela:

```text
ecommerce.resumo_diario_categoria
```

A tabela é particionada por:

```text
data
```

Exemplo:

```text
data=2026-09-24
data=2026-09-25
```

---

# Consultando o Hive

Para consultar as partições e os dados:

```powershell
docker compose -f docker-compose.yml -f docker-compose.batch.yml exec spark /opt/spark/bin/spark-sql --master "local[2]" --conf spark.sql.catalogImplementation=hive --conf spark.sql.session.timeZone=UTC --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 -e "SHOW PARTITIONS ecommerce.resumo_diario_categoria; SELECT * FROM ecommerce.resumo_diario_categoria ORDER BY data, category;"
```

Exemplo de resultado:

```text
data=2026-09-24
data=2026-09-25
```

Essa consulta pode ser executada em uma nova sessão depois do encerramento do job Spark, comprovando que os metadados e os dados permanecem disponíveis.

---

# Reprocessamento

Uma data pode ser executada novamente sem acumular os valores anteriores.

Ao reprocessar uma determinada data:

```text
2026-09-25
```

somente o resultado daquela partição é substituído.

As demais datas permanecem armazenadas.

Esse comportamento permite reprocessamento sem duplicar os indicadores.

---

# Teste controlado

O arquivo:

```text
spark/test-data/teste_controlado_2026-09-24.json
```

foi criado para validar os requisitos principais do ETL com valores previamente conhecidos.

A amostra contém:

- eventos `product_view`;
- eventos `purchase`;
- um evento duplicado;
- um registro inválido;
- um registro pertencente a outra data;
- uma categoria presente somente em visualizações;
- uma categoria presente somente em compras.

---

## Executando o teste controlado

```powershell
docker compose -f docker-compose.yml -f docker-compose.batch.yml exec -e DATA_PROCESSAMENTO=2026-09-24 -e CAMINHO_ENTRADA=hdfs://namenode:9000/ecommerce/historico/teste_controlado_2026-09-24.json spark /opt/spark/bin/spark-shell --master "local[2]" --conf spark.sql.catalogImplementation=hive --conf spark.sql.session.timeZone=UTC --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 -i /workspace/etl_resumo_diario.scala
```

---

## Quantidades esperadas no teste

A amostra possui:

```text
9 registros lidos
1 registro inválido
8 registros válidos
7 registros após deduplicação
6 registros pertencentes à data 2026-09-24
```

Resultado esperado:

| Data | Categoria | Visualizações | Compras | Itens | Valor |
|---|---|---:|---:|---:|---:|
| 2026-09-24 | audio | 2 | 2 | 5 | 176.50 |
| 2026-09-24 | perifericos | 1 | 0 | 0 | 0.00 |
| 2026-09-24 | telefonia | 0 | 1 | 2 | 600.00 |

O resultado confirma que:

- o registro inválido é descartado;
- a duplicata não altera os totais;
- o evento de outra data é ignorado;
- `perifericos` permanece mesmo possuindo somente visualizações;
- `telefonia` permanece mesmo possuindo somente compras;
- o `FULL OUTER JOIN` preserva registros dos dois lados.

---

# Resultado de referência — 2026-09-25

Durante os testes com a amostra histórica principal, foram obtidos os seguintes indicadores:

| Categoria | Visualizações | Compras | Itens | Valor |
|---|---:|---:|---:|---:|
| audio | 16 | 18 | 31 | 6196.90 |
| informatica | 42 | 42 | 87 | 184900.00 |
| perifericos | 31 | 45 | 102 | 25840.00 |
| telefonia | 30 | 20 | 39 | 85800.00 |

A execução repetida da mesma data manteve os mesmos resultados, comprovando que o reprocessamento não duplica os indicadores.

---

# Critérios validados

Durante os testes foram validados:

- leitura de JSON armazenado no HDFS;
- gravação de tabela registrada no Hive;
- consulta da tabela em uma nova sessão;
- persistência do Hive Metastore;
- processamento por data informada;
- processamento automático D-1 em UTC;
- uso do campo `timestamp` para filtro;
- tipos explícitos;
- uso de decimal para valores monetários;
- descarte de registros inválidos;
- remoção de duplicatas por `event_id`;
- agregação de visualizações;
- agregação de compras;
- soma de unidades compradas;
- cálculo do valor das compras;
- `FULL OUTER JOIN`;
- categorias presentes apenas em um dos lados;
- shuffle durante as agregações;
- persistência particionada por data;
- reprocessamento sem duplicação;
- preservação das demais datas processadas.

---

## Encerrando o Spark Shell

Após uma execução utilizando `spark-shell`:

```scala
:quit
```

---

## Observação sobre o Hive Metastore

Durante alguns testes o Spark apresentou uma mensagem de reconexão do cliente do Hive Metastore:

```text
MetaStoreClient lost connection.
Attempting to reconnect
```

O cliente conseguiu se reconectar e as operações foram concluídas normalmente.

As partições permaneceram registradas e foram consultadas posteriormente através de uma nova sessão Spark SQL.

---

## Escopo

Este módulo é responsável somente pelo processamento batch histórico e persistência dos indicadores.

Não fazem parte desta implementação:

- dashboard;
- agendador;
- taxa de conversão;
- alterações no contrato do gerador;
- cluster Spark distribuído.