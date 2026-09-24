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
