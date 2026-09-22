import json
import os
import random
import time
import uuid
from datetime import datetime

USUARIOS = [f"user_{i:03d}" for i in range(1, 51)]

PRODUTOS = [
    {"id": "prod_001", "nome": "Notebook", "preco": 3500.00},
    {"id": "prod_002", "nome": "Smartphone", "preco": 2200.00},
    {"id": "prod_003", "nome": "Fone Bluetooth", "preco": 199.90},
    {"id": "prod_004", "nome": "Teclado Mecânico", "preco": 350.00},
    {"id": "prod_005", "nome": "Mouse Gamer", "preco": 180.00},
    {"id": "prod_006", "nome": "Monitor", "preco": 1200.00}
]

EVENTOS = [
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
    "payment_approved",
    "shipping",
    "delivered"
]


def gerar_evento():

    usuario = random.choice(USUARIOS)
    produto = random.choice(PRODUTOS)
    evento = random.choice(EVENTOS)

    dados = {
        "event_id": str(uuid.uuid4()),
        "user_id": usuario,
        "product_id": produto["id"],
        "product_name": produto["nome"],
        "event_type": evento,
        "timestamp": datetime.now().isoformat(),
        "price": produto["preco"],
        "quantity": random.randint(1, 3)
    }

    return dados

## Cria a pasta dados
os.makedirs ("dados", exist_ok=True)
# Geração contínua
while True:

    evento = gerar_evento()

    # Mostra no terminal
    print(json.dumps(evento), flush=True)

    # Salva no arquivo
    with open("dados/eventos.json", "a", encoding="utf-8") as arquivo:
        arquivo.write(json.dumps(evento) + "\n")

    # Aguarda entre 0,5 e 2 segundos
    time.sleep(random.uniform(0.5, 2))