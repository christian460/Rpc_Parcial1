import os
import pika
import time

def on_request(ch, method, props, body):
    message = body.decode()
    print(f" [.] Petición recibida: '{message}'")
    response = (
        f"¡Hola! El servidor procesó tu mensaje: "
        f"'{message.upper()}'"
    )
    time.sleep(1)
    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id
        ),
        body=response
    )
    ch.basic_ack(
        delivery_tag=method.delivery_tag
    )
    print(" [x] Respuesta enviada.")

def main():
    rabbitmq_url = os.getenv("RABBITMQ_URL")
    if rabbitmq_url:
        params = pika.URLParameters(rabbitmq_url)
        connection = pika.BlockingConnection(params)
    else:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='127.0.0.1'
            )
        )
    channel = connection.channel()
    channel.queue_declare(
        queue='rpc_queue'
    )
    channel.basic_qos(
        prefetch_count=1
    )
    channel.basic_consume(
        queue='rpc_queue',
        on_message_callback=on_request
    )
    print(
        " [x] Servidor RPC listo. "
        "Esperando peticiones en 'rpc_queue'..."
    )
    print(" [!] Presiona CTRL+C para salir.")
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nDeteniendo servidor...")