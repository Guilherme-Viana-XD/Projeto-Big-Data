import json
import sys
from collections import Counter
from datetime import datetime, timezone


PRODUTOS = {
    "prod_001": {
        "nome": "Notebook",
        "categoria": "informatica"
    },
    "prod_002": {
        "nome": "Smartphone",
        "categoria": "telefonia"
    },
    "prod_003": {
        "nome": "Fone Bluetooth",
        "categoria": "audio"
    },
    "prod_004": {
        "nome": "Teclado Mecânico",
        "categoria": "perifericos"
    },
    "prod_005": {
        "nome": "Mouse Gamer",
        "categoria": "perifericos"
    },
    "prod_006": {
        "nome": "Monitor",
        "categoria": "informatica"
    }
}


CAMPOS_OBRIGATORIOS = [
    "event_id",
    "user_id",
    "product_id",
    "product_name",
    "event_type",
    "timestamp",
    "price",
    "quantity",
    "category"
]


TIPOS_EVENTOS = {
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
    "payment_approved",
    "shipping",
    "delivered"
}


def validar_timestamp(valor):
    try:
        if not isinstance(valor, str):
            return False

        if not valor.endswith("Z"):
            return False

        datetime.fromisoformat(valor.replace("Z", "+00:00"))
        return True

    except (ValueError, TypeError):
        return False


def main():
    if len(sys.argv) != 2:
        print("Uso: python scripts/validar_eventos.py <arquivo.json>")
        sys.exit(1)

    caminho = sys.argv[1]

    total = 0
    linhas_invalidas = 0
    ids = set()
    ids_duplicados = 0
    campos_invalidos = 0
    timestamps_invalidos = 0
    categorias_invalidas = 0
    timestamps_fora_de_ordem = 0

    tipos_eventos = Counter()
    problemas = []

    timestamp_anterior = None

    try:
        arquivo = open(caminho, "r", encoding="utf-8")
    except FileNotFoundError:
        print(f"Arquivo não encontrado: {caminho}")
        sys.exit(1)

    with arquivo:
        for numero_linha, linha in enumerate(arquivo, start=1):
            total += 1

            try:
                evento = json.loads(linha)
            except json.JSONDecodeError:
                linhas_invalidas += 1

                if len(problemas) < 10:
                    problemas.append(
                        f"Linha {numero_linha}: JSON inválido"
                    )

                continue

            linha_tem_problema = False

            # Campos obrigatórios
            campos_faltando = [
                campo
                for campo in CAMPOS_OBRIGATORIOS
                if campo not in evento
            ]

            if campos_faltando:
                campos_invalidos += 1
                linha_tem_problema = True

                if len(problemas) < 10:
                    problemas.append(
                        f"Linha {numero_linha}: campos ausentes: "
                        f"{', '.join(campos_faltando)}"
                    )

            # Tipos básicos
            if not isinstance(evento.get("event_id"), str):
                campos_invalidos += 1
                linha_tem_problema = True

            if not isinstance(evento.get("user_id"), str):
                campos_invalidos += 1
                linha_tem_problema = True

            if not isinstance(evento.get("product_id"), str):
                campos_invalidos += 1
                linha_tem_problema = True

            if not isinstance(evento.get("product_name"), str):
                campos_invalidos += 1
                linha_tem_problema = True

            if not isinstance(evento.get("event_type"), str):
                campos_invalidos += 1
                linha_tem_problema = True

            if not isinstance(evento.get("category"), str):
                campos_invalidos += 1
                linha_tem_problema = True

            if not isinstance(evento.get("price"), (int, float)):
                campos_invalidos += 1
                linha_tem_problema = True

            if (
                not isinstance(evento.get("quantity"), int)
                or isinstance(evento.get("quantity"), bool)
                or evento.get("quantity", 0) <= 0
            ):
                campos_invalidos += 1
                linha_tem_problema = True

            # ID duplicado
            event_id = evento.get("event_id")

            if isinstance(event_id, str):
                if event_id in ids:
                    ids_duplicados += 1
                    linha_tem_problema = True

                    if len(problemas) < 10:
                        problemas.append(
                            f"Linha {numero_linha}: "
                            f"event_id duplicado: {event_id}"
                        )

                ids.add(event_id)

            # Tipo de evento
            event_type = evento.get("event_type")

            if event_type in TIPOS_EVENTOS:
                tipos_eventos[event_type] += 1
            else:
                campos_invalidos += 1
                linha_tem_problema = True

            # Timestamp
            timestamp = evento.get("timestamp")

            if not validar_timestamp(timestamp):
                timestamps_invalidos += 1
                linha_tem_problema = True

                if len(problemas) < 10:
                    problemas.append(
                        f"Linha {numero_linha}: timestamp inválido"
                    )
            else:
                timestamp_atual = datetime.fromisoformat(
                    timestamp.replace("Z", "+00:00")
                )

                if (
                    timestamp_anterior is not None
                    and timestamp_atual < timestamp_anterior
                ):
                    timestamps_fora_de_ordem += 1

                timestamp_anterior = timestamp_atual

            # Categoria do produto
            product_id = evento.get("product_id")

            if product_id in PRODUTOS:
                produto = PRODUTOS[product_id]

                if evento.get("product_name") != produto["nome"]:
                    categorias_invalidas += 1
                    linha_tem_problema = True

                    if len(problemas) < 10:
                        problemas.append(
                            f"Linha {numero_linha}: "
                            f"nome do produto incorreto"
                        )

                if evento.get("category") != produto["categoria"]:
                    categorias_invalidas += 1
                    linha_tem_problema = True

                    if len(problemas) < 10:
                        problemas.append(
                            f"Linha {numero_linha}: "
                            f"categoria incorreta para {product_id}"
                        )

            else:
                categorias_invalidas += 1
                linha_tem_problema = True

                if len(problemas) < 10:
                    problemas.append(
                        f"Linha {numero_linha}: product_id desconhecido"
                    )

            if linha_tem_problema:
                linhas_invalidas += 1

    print("\n===== VALIDAÇÃO DOS EVENTOS =====")
    print(f"Arquivo: {caminho}")
    print(f"Total de registros: {total}")
    print(f"Linhas inválidas: {linhas_invalidas}")
    print(f"IDs duplicados: {ids_duplicados}")
    print(f"Campos/tipos inválidos: {campos_invalidos}")
    print(f"Timestamps inválidos: {timestamps_invalidos}")
    print(f"Categorias/produtos inválidos: {categorias_invalidas}")
    print(f"Timestamp fora de ordem: {timestamps_fora_de_ordem}")

    print("\n===== EVENTOS POR TIPO =====")

    for tipo, quantidade in sorted(tipos_eventos.items()):
        print(f"{tipo}: {quantidade}")

    print("\n===== PROBLEMAS ENCONTRADOS =====")

    if problemas:
        for problema in problemas:
            print(f"- {problema}")
    else:
        print("Nenhum problema encontrado.")

    print("\n===== RESULTADO =====")

    if (
        linhas_invalidas == 0
        and ids_duplicados == 0
        and campos_invalidos == 0
        and timestamps_invalidos == 0
        and categorias_invalidas == 0
    ):
        print("VALIDAÇÃO OK")
    else:
        print("VALIDAÇÃO COM PROBLEMAS")


if __name__ == "__main__":
    main()
