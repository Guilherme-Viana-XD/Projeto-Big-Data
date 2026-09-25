import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone


USUARIOS = [f"user_{i:03d}" for i in range(1, 51)]

PRODUTOS = [
    {"id": "prod_001", "nome": "Notebook", "preco": 3500.00, "category": "informatica"},
    {"id": "prod_002", "nome": "Smartphone", "preco": 2200.00, "category": "telefonia"},
    {"id": "prod_003", "nome": "Fone Bluetooth", "preco": 199.90, "category": "audio"},
    {"id": "prod_004", "nome": "Teclado Mecânico", "preco": 350.00, "category": "perifericos"},
    {"id": "prod_005", "nome": "Mouse Gamer", "preco": 180.00, "category": "perifericos"},
    {"id": "prod_006", "nome": "Monitor", "preco": 1200.00, "category": "informatica"},
]

EVENTOS = [
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
    "payment_approved",
    "shipping",
    "delivered",
]


def timestamp_utc(momento=None):
    if momento is None:
        momento = datetime.now(timezone.utc)

    return momento.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def gerar_evento(timestamp=None):
    usuario = random.choice(USUARIOS)
    produto = random.choice(PRODUTOS)
    evento = random.choice(EVENTOS)

    dados = {
        "event_id": str(uuid.uuid4()),
        "user_id": usuario,
        "product_id": produto["id"],
        "product_name": produto["nome"],
        "event_type": evento,
        "timestamp": timestamp_utc(timestamp),
        "price": produto["preco"],
        "quantity": random.randint(1, 3),
        "category": produto["category"],
    }

    return dados


def timestamp_streaming():
    agora = datetime.now(timezone.utc)

    if random.random() < 0.10:
        atraso = random.randint(5, 45)
        return agora - timedelta(seconds=atraso)

    return agora


def gerar_streaming():
    os.makedirs("dados", exist_ok=True)

    print("Gerador em modo contínuo. Pressione Ctrl+C para parar.")

    try:
        while True:
            evento = gerar_evento(timestamp_streaming())

            print(json.dumps(evento, ensure_ascii=False), flush=True)

            with open("dados/eventos.json", "a", encoding="utf-8") as arquivo:
                arquivo.write(
                    json.dumps(evento, ensure_ascii=False) + "\n"
                )

            time.sleep(random.uniform(0.5, 2))

    except KeyboardInterrupt:
        print("\nGerador interrompido.")


def gerar_historico(data, total, saida):
    try:
        inicio = datetime.strptime(data, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        raise ValueError(
            "A data deve estar no formato YYYY-MM-DD."
        )

    fim = inicio + timedelta(days=1)

    diretorio = os.path.dirname(saida)

    if diretorio:
        os.makedirs(diretorio, exist_ok=True)

    with open(saida, "w", encoding="utf-8") as arquivo:
        for _ in range(total):
            intervalo = random.uniform(
                0,
                (fim - inicio).total_seconds()
            )

            momento = inicio + timedelta(seconds=intervalo)

            evento = gerar_evento(momento)

            arquivo.write(
                json.dumps(evento, ensure_ascii=False) + "\n"
            )

    print(f"Histórico gerado: {saida}")
    print(f"Total de eventos: {total}")


def main():
    parser = argparse.ArgumentParser(
        description="Gerador de eventos para o projeto de Big Data."
    )

    parser.add_argument(
        "--historico",
        help="Gera um histórico para a data informada (YYYY-MM-DD)."
    )

    parser.add_argument(
        "--total",
        type=int,
        default=10000,
        help="Quantidade de eventos do histórico."
    )

    parser.add_argument(
        "--saida",
        default="dados/historico.json",
        help="Arquivo de saída do histórico."
    )

    args = parser.parse_args()

    if args.historico:
        if args.total <= 0:
            parser.error("--total deve ser maior que zero.")

        gerar_historico(
            args.historico,
            args.total,
            args.saida
        )
    else:
        gerar_streaming()


if __name__ == "__main__":
    main()
