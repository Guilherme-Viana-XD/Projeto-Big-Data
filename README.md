# Gerador de Eventos - MVP Big Data

Gerador de eventos sintéticos para o projeto de Big Data.

O gerador possui dois modos:

- geração contínua de eventos para processamento em streaming;
- geração de dados históricos para processamento em batch.

## Estrutura

```text
gerador/
└── gerador.py

dados/
├── eventos.json
└── historico_2026-09-21.json

scripts/
└── validar_eventos.py

Geração contínua

Para iniciar o gerador:

python .\gerador\gerador.py

Os eventos são exibidos no terminal e adicionados ao arquivo:

dados/eventos.json

O arquivo utiliza o formato JSON Lines, com um objeto JSON por linha.

Para interromper a geração:

Ctrl + C

O arquivo dados/eventos.json é utilizado pelo Flume como fonte dos eventos.

Geração histórica

Para gerar 10.000 eventos referentes a uma data:

python .\gerador\gerador.py --historico 2026-09-21 --total 10000 --saida dados\historico_2026-09-21.json

O modo histórico:

gera exatamente a quantidade solicitada;
utiliza timestamps em UTC;
mantém os eventos dentro da data informada;
termina automaticamente;
grava em um arquivo separado;
substitui o arquivo de saída caso ele já exista.
Validação

Para validar um arquivo de eventos:

python .\scripts\validar_eventos.py .\dados\historico_2026-09-21.json

O validador verifica:

quantidade de registros;
linhas inválidas;
IDs duplicados;
campos obrigatórios;
tipos dos campos;
timestamps;
categorias e produtos;
quantidade de eventos por tipo;
timestamps fora de ordem.
Formato dos eventos

Exemplo:

{
  "event_id": "67f90465-ab29-44a5-bb58-fa3aba381503",
  "user_id": "user_037",
  "product_id": "prod_001",
  "product_name": "Notebook",
  "event_type": "page_view",
  "timestamp": "2026-09-23T19:24:49.897Z",
  "price": 3500.0,
  "quantity": 2,
  "category": "informatica"
}
Campos
event_id: identificador único do evento.
user_id: identificador do usuário.
product_id: identificador do produto.
product_name: nome do produto.
event_type: tipo do evento.
timestamp: data e hora do evento em UTC.
price: preço do produto.
quantity: quantidade associada ao evento.
category: categoria fixa do produto.
Tipos de eventos

Os eventos utilizados pelo gerador são:

page_view
product_view
add_to_cart
remove_from_cart
purchase
payment_approved
shipping
delivered

Os eventos são sintéticos, aleatórios e independentes. O gerador não simula uma jornada real de compra.

Timestamps fora de ordem

No modo contínuo, aproximadamente 10% dos eventos recebem um timestamp atrasado entre 5 e 45 segundos.

Isso permite simular eventos chegando ao pipeline fora da ordem cronológica, cenário útil para testes de processamento de streaming.

Testes realizados
Histórico

Arquivo:

dados/historico_2026-09-21.json

Resultado:

10.000 registros
0 linhas inválidas
0 IDs duplicados
0 campos/tipos inválidos
0 timestamps inválidos
0 categorias/produtos inválidos
VALIDAÇÃO OK
Streaming

Arquivo:

dados/eventos.json

Teste realizado com 22 eventos:

22 registros
0 linhas inválidas
0 IDs duplicados
0 campos/tipos inválidos
0 timestamps inválidos
0 categorias/produtos inválidos
3 timestamps fora de ordem
VALIDAÇÃO OK