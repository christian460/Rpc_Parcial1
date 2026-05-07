import pika
import time

def on_request(ch, method, props, body):
    message = body.decode()
    print(f" [.] Petición recibida: '{message}'")

    # Procesamiento simple: Convertir a mayúsculas y simular un pequeño retraso
    response = f"¡Hola! El servidor procesó tu mensaje: '{message.upper()}'"
    time.sleep(1) 

    # Enviar la respuesta de vuelta a la cola que nos indicó el cliente (reply_to)
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=response)
    
    # Confirmar que el mensaje fue procesado
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f" [x] Respuesta enviada.")

def main():
    # Conexión local a RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
    channel = connection.channel()

    # Declarar la cola de la que vamos a leer
    channel.queue_declare(queue='rpc_queue')

    # No aceptar más de un mensaje a la vez hasta que terminemos el anterior
    channel.basic_qos(prefetch_count=1)
    
    # Configurar el consumo
    channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

    print(" [x] Servidor RPC listo. Esperando peticiones en 'rpc_queue'...")
    print(" [!] Presiona CTRL+C para salir.")
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nDeteniendo servidor...")
