import os
import pika
import uuid
from time import sleep
from flask import Flask

app = Flask(__name__)

class RpcClient(object):

    def __init__(self, rpc_queue):
        self.rpc_queue = rpc_queue

    def create_connection(self):

        rabbitmq_url = os.getenv("RABBITMQ_URL")

        if rabbitmq_url:

            params = pika.URLParameters(rabbitmq_url)

            connection = pika.BlockingConnection(params)

        else:

            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='127.0.0.1')
            )

        return connection

    def call(self, payload, timeout=10):

        connection = self.create_connection()

        channel = connection.channel()

        channel.queue_declare(queue=self.rpc_queue)

        result = channel.queue_declare(
            queue='',
            exclusive=True
        )

        callback_queue = result.method.queue

        corr_id = str(uuid.uuid4())

        response = None

        def on_response(ch, method, props, body):

            nonlocal response

            if props.correlation_id == corr_id:

                response = body.decode()

        channel.basic_consume(
            queue=callback_queue,
            on_message_callback=on_response,
            auto_ack=True
        )

        channel.basic_publish(
            exchange='',
            routing_key=self.rpc_queue,
            properties=pika.BasicProperties(
                reply_to=callback_queue,
                correlation_id=corr_id
            ),
            body=payload
        )

        waited = 0

        while response is None and waited < timeout:

            connection.process_data_events(
                time_limit=0.5
            )

            sleep(0.1)

            waited += 0.6

        connection.close()

        return response

rpc_client = RpcClient('rpc_queue')

@app.route('/')
def home():
    return 'Servidor RPC activo'

@app.route('/rpc_call/<payload>')
def rpc_call(payload):

    try:

        response = rpc_client.call(payload)

        if response is None:

            return (
                'Timeout waiting for RPC response',
                504
            )

        return response

    except Exception as e:

        print("RPC Error:", e)

        return (
            'RabbitMQ connection error',
            500
        )

if __name__ == '__main__':

    app.run(
        host='0.0.0.0',
        port=5000
    )