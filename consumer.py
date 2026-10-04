import json
import os

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "admin")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "admin")

EXCHANGE = "transfermarkt.exchange"
QUEUE = "transfermarkt.transferencia.queue"
ROUTING_KEY = "transfermarkt.transferencia"


def formatar_data(data_transferencia):
    if isinstance(data_transferencia, str):
        return data_transferencia
    if isinstance(data_transferencia, list) and len(data_transferencia) == 3:
        ano, mes, dia = data_transferencia
        return f"{ano:04d}-{mes:02d}-{dia:02d}"
    return str(data_transferencia)


def formatar_valor(valor_transferencia):
    try:
        valor = float(valor_transferencia)
        return f"€ {valor:,.2f}"
    except (TypeError, ValueError):
        return f"€ {valor_transferencia}"


def exibir_transferencia(transferencia):
    print("=" * 58)
    print("NOVA TRANSFERÊNCIA REGISTRADA NO MERCADO")
    print("=" * 58)
    print(f"Jogador : {transferencia.get('jogadorNome', 'desconhecido')}")
    print(f"Origem  : {transferencia.get('clubeOrigem', 'desconhecido')}")
    print(f"Destino : {transferencia.get('clubeDestino', 'desconhecido')}")
    print(f"Valor   : {formatar_valor(transferencia.get('valorTransferencia'))}")
    print(f"Data    : {formatar_data(transferencia.get('dataTransferencia'))}")
    print("=" * 58)


def processar_transferencia(ch, method, properties, body):
    try:
        transferencia = json.loads(body)
    except json.JSONDecodeError:
        print(f"AVISO: mensagem com JSON inválido foi descartada: {body!r}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    exibir_transferencia(transferencia)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD),
            )
        )
    except pika.exceptions.AMQPConnectionError:
        print(f"ERRO: não foi possível conectar ao RabbitMQ em {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        print("Dica: verifique se o container está ativo (docker compose up -d)")
        return

    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=ROUTING_KEY)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=processar_transferencia)

    print("[*] Consumer Transfermarkt iniciado")
    print(f"[*] Aguardando mensagens na fila '{QUEUE}'... (CTRL+C para encerrar)")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n[*] Encerrando consumer...")
        channel.stop_consuming()
        connection.close()
        print("[*] Consumer encerrado.")


if __name__ == "__main__":
    main()
