# Flink Streaming — Alertas por Categoria

Este módulo implementa o processamento em tempo real do projeto de Big Data utilizando **Apache Flink**, com leitura contínua dos eventos armazenados no **HDFS** e persistência dos alertas no **HBase**.

## Objetivo

Identificar categorias de produtos com alto interesse a partir de eventos do tipo:

```text
product_view
```

O processamento utiliza:

- tempo do próprio evento (`event time`);
- watermark para eventos fora de ordem;
- janela deslizante;
- contagem por categoria;
- geração de alertas;
- armazenamento no HBase.

---

## Fluxo

```text
Gerador Python
      ↓
Apache Flume
      ↓
HDFS /ecommerce/raw
      ↓
Apache Flink
      ↓
product_view
      ↓
Watermark de 60 segundos
      ↓
Janela deslizante de 60 segundos
Avanço a cada 10 segundos
      ↓
Contagem por categoria
      ↓
Limite de alerta
      ↓
HBase
alertas_categoria
```

---

## Arquivos

### `Dockerfile`

Cria a imagem utilizada pelo Flink com as dependências necessárias para integração com:

- HDFS;
- HBase.

### `leitura_raw.sql`

Job auxiliar utilizado para validar a leitura contínua dos arquivos existentes em:

```text
hdfs://namenode:9000/ecommerce/raw
```

O diretório é monitorado periodicamente, permitindo que novos arquivos finalizados pelo Flume sejam descobertos sem reiniciar o job.

### `alertas_categoria.sql`

Job principal do módulo.

Responsável por:

1. Ler continuamente os eventos do HDFS;
2. Interpretar o campo `timestamp` como horário do evento;
3. Aplicar watermark de 60 segundos;
4. Filtrar eventos `product_view`;
5. Criar janelas deslizantes;
6. Contar visualizações por categoria;
7. Identificar categorias que atingem o limite;
8. Persistir os alertas no HBase.

### `test-data/`

Contém eventos controlados utilizados para validar:

- janelas;
- eventos fora de ordem;
- watermark;
- limite de alerta.

---

# Configuração da janela

O processamento utiliza:

```text
Tamanho da janela: 60 segundos
Avanço da janela: 10 segundos
Watermark: 60 segundos
```

No Flink SQL:

```sql
HOP(
    TABLE eventos_raw,
    DESCRIPTOR(`timestamp`),
    INTERVAL '10' SECOND,
    INTERVAL '60' SECOND
)
```

Como as janelas são sobrepostas, um mesmo evento pode participar de mais de uma janela.

Exemplo:

```text
22:09:20 → 22:10:20
22:09:30 → 22:10:30
22:09:40 → 22:10:40
```

---

# Watermark e eventos fora de ordem

O job utiliza:

```sql
WATERMARK FOR `timestamp`
    AS `timestamp` - INTERVAL '60' SECOND
```

Isso permite que eventos cheguem fora da ordem cronológica.

Durante o teste controlado, por exemplo, os eventos foram recebidos nesta ordem:

```text
22:10:05
22:10:15
22:09:50
22:10:18
```

O evento de `22:09:50` chegou depois de eventos com horário maior, mas ainda pôde ser considerado porque estava dentro da tolerância definida pela watermark.

## Eventos tardios

A watermark não representa tolerância ilimitada.

Quando ela ultrapassa o fim de uma janela, essa janela pode ser finalizada pelo Flink.

Eventos cujo timestamp cheguem depois desse fechamento são considerados tardios para aquela janela e não atualizam o resultado já finalizado.

---

# Limite de alerta

O limite padrão utilizado no MVP é:

```text
2 product_view
```

Ele está centralizado no início do arquivo `alertas_categoria.sql`:

```sql
CREATE TEMPORARY VIEW configuracao_alerta AS
SELECT CAST(2 AS BIGINT) AS limite_alerta;
```

Para alterar o limite, basta substituir o valor `2`.

Exemplo:

```sql
SELECT CAST(5 AS BIGINT) AS limite_alerta;
```

---

# HBase

Os alertas são armazenados na tabela:

```text
alertas_categoria
```

Família de colunas:

```text
info
```

Os campos armazenados são:

```text
categoria
inicio_janela
fim_janela
visualizacoes
limite_alerta
```

## Row key

A chave utilizada é determinística:

```text
categoria|inicio_janela|fim_janela
```

Exemplo:

```text
informatica|2026-09-26 22:09:20.000|2026-09-26 22:10:20.000
```

Essa estratégia permite identificar unicamente um alerta de determinada categoria em determinada janela.

---

# Inicialização

A partir da raiz do projeto:

```powershell
docker compose `
  -f docker-compose.yml `
  -f docker-compose.batch.yml `
  -f docker-compose.streaming.yml `
  up -d
```

Verifique os serviços:

```powershell
docker compose `
  -f docker-compose.yml `
  -f docker-compose.batch.yml `
  -f docker-compose.streaming.yml `
  ps
```

O ambiente integrado contém:

```text
namenode
datanode
flume
hbase
metastore-db
hive-metastore
spark
jobmanager
taskmanager
```

---

# Criar tabela no HBase

Caso a tabela ainda não exista:

```powershell
"create 'alertas_categoria', 'info'" | docker compose -f docker-compose.yml -f docker-compose.streaming.yml exec -T hbase hbase shell -n
```

Para verificar:

```powershell
"describe 'alertas_categoria'" | docker compose -f docker-compose.yml -f docker-compose.streaming.yml exec -T hbase hbase shell -n
```

---

# Executar leitura simples

Para verificar se o Flink está recebendo os eventos existentes no HDFS:

```powershell
docker compose `
  -f docker-compose.yml `
  -f docker-compose.streaming.yml `
  exec jobmanager `
  /opt/flink/bin/sql-client.sh `
  -f /workspace/flink/leitura_raw.sql
```

O job monitora:

```text
hdfs://namenode:9000/ecommerce/raw
```

---

# Executar o job de alertas

Com o ambiente iniciado e a tabela HBase criada:

```powershell
docker compose `
  -f docker-compose.yml `
  -f docker-compose.batch.yml `
  -f docker-compose.streaming.yml `
  exec jobmanager `
  /opt/flink/bin/sql-client.sh `
  -f /workspace/flink/alertas_categoria.sql
```

Após a submissão, será retornado um Job ID.

---

# Verificar jobs do Flink

```powershell
docker compose `
  -f docker-compose.yml `
  -f docker-compose.batch.yml `
  -f docker-compose.streaming.yml `
  exec jobmanager `
  /opt/flink/bin/flink list
```

O job de alertas deve aparecer como:

```text
RUNNING
```

---

# Consultar alertas no HBase

```powershell
"scan 'alertas_categoria'" | docker compose -f docker-compose.yml -f docker-compose.batch.yml -f docker-compose.streaming.yml exec -T hbase hbase shell -n
```

Exemplo de resultado:

```text
informatica|2026-09-26 22:09:20.000|2026-09-26 22:10:20.000

info:categoria        informatica
info:inicio_janela    2026-09-26 22:09:20.000
info:fim_janela       2026-09-26 22:10:20.000
info:visualizacoes    3
info:limite_alerta    2
```

---

# Teste controlado

Os arquivos:

```text
test-data/teste_janelas_1.json
test-data/teste_janelas_avanco.json
```

foram utilizados para validar o funcionamento das janelas e watermarks.

O teste gerou seis janelas para a categoria `informatica`:

```text
22:09:10 → 22:10:10   2 visualizações
22:09:20 → 22:10:20   3 visualizações
22:09:30 → 22:10:30   3 visualizações
22:09:40 → 22:10:40   3 visualizações
22:09:50 → 22:10:50   3 visualizações
22:10:00 → 22:11:00   2 visualizações
```

A categoria `audio` possuía apenas uma visualização e, portanto, não atingiu o limite de alerta.

---

# Persistência

Os dados do HBase são armazenados em um volume Docker:

```text
hbase_data
```

Assim, os alertas permanecem armazenados mesmo quando o container é reiniciado.

---

# Limitações do MVP

Esta implementação foi reduzida ao escopo necessário para demonstrar o pipeline de streaming.

Não foram implementados:

- dashboard;
- análise de abandono de carrinho;
- funil de vendas;
- processamento logístico avançado;
- deduplicação global dos eventos.

O job utiliza os arquivos presentes no HDFS como fonte. Em cenários de recriação completa do estado do Flink ou reprocessamento do diretório, arquivos anteriores podem voltar a ser processados.

A row key determinística reduz duplicações na tabela de alertas para a mesma combinação de categoria e janela, pois o mesmo identificador é reutilizado.

---

# Integração com o processamento batch

O projeto possui dois fluxos complementares que utilizam o HDFS como camada compartilhada de armazenamento:

```text
Streaming:
Gerador → Flume → HDFS /ecommerce/raw → Flink → HBase

Batch:
HDFS /ecommerce/historico → Spark → Hive
```

O fluxo de streaming processa continuamente os eventos enviados para `/ecommerce/raw`, enquanto o fluxo batch utiliza os dados históricos para gerar agregações analíticas.

## Inicialização automática do diretório HDFS

O arquivo `docker-compose.streaming.yml` possui o serviço `hdfs-init`.

Esse serviço aguarda o NameNode ficar disponível e cria automaticamente o diretório:

```text
/ecommerce/raw
```

O JobManager do Flink somente é iniciado após a conclusão bem-sucedida do `hdfs-init`.

Isso evita que o job entre em estado `RESTARTING` quando o diretório `/ecommerce/raw` ainda não existe no HDFS.

## Validação do fluxo de streaming

O fluxo foi validado de ponta a ponta:

```text
Gerador
→ Flume
→ HDFS
→ Flink
→ HBase
```

Foram verificados:

- gravação dos eventos pelo Flume em `/ecommerce/raw`;
- leitura contínua dos arquivos pelo Flink;
- processamento utilizando `event time`;
- watermark com tolerância de 60 segundos;
- janela deslizante de 60 segundos com avanço de 10 segundos;
- filtro de eventos `product_view`;
- contagem das visualizações por categoria;
- geração de alerta quando `visualizacoes >= 2`;
- escrita dos alertas na tabela HBase `alertas_categoria`;
- uso de row key determinística;
- persistência dos dados do HBase após reinicialização do container.

## Teste controlado de watermark e janelas

Os arquivos utilizados no teste controlado são:

```text
flink/test-data/teste_janelas_1.json
flink/test-data/teste_janelas_avanco.json
```

O primeiro arquivo contém eventos `product_view`, incluindo eventos fora de ordem.

O segundo arquivo contém um evento com timestamp posterior utilizado para avançar a watermark e provocar o fechamento das janelas.

No cenário validado foram geradas 6 linhas de alerta para a categoria `informatica`.

As contagens observadas foram compatíveis com as janelas deslizantes de 60 segundos e avanço de 10 segundos.

## Persistência no HBase

A tabela utilizada é:

```text
alertas_categoria
```

A família de colunas utilizada é:

```text
info
```

A row key segue o formato:

```text
categoria|inicio_janela|fim_janela
```

Os dados do HBase são armazenados no volume Docker `hbase_data`.

Foi realizado um restart isolado do container HBase e as linhas previamente gravadas permaneceram disponíveis, confirmando a persistência dos dados.
