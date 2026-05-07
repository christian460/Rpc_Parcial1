import os
import pika
import uuid
import threading
from time import sleep
from flask import Flask

app = Flask(__name__)

class RpcClient(object):
    def __init__(self, rpc_queue):
        self.internal_lock = threading.Lock()
        self.queue = {}
        self.callbacks = {}
        self.rpc_queue = rpc_queue
        rabbitmq_url = os.getenv("RABBITMQ_URL")
        if rabbitmq_url:
            params = pika.URLParameters(rabbitmq_url)
            self.connection = pika.BlockingConnection(params)
        else:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host='127.0.0.1')
            )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.rpc_queue)
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self._on_response,
            auto_ack=True
        )
        thread = threading.Thread(
            target=self._process_data_events,
            daemon=True
        )
        thread.start()

    def _process_data_events(self):
        while True:
            try:
                self.connection.process_data_events(time_limit=0.5)
            except Exception as e:
                print("Consumer error:", e)
            sleep(0.05)

    def _on_response(self, ch, method, props, body):
        response = body.decode() if isinstance(body, bytes) else body
        with self.internal_lock:
            self.queue[props.correlation_id] = response
            callback = self.callbacks.pop(
                props.correlation_id,
                None
            )
        if callback:
            callback(props.correlation_id, response)

    def wait_for_response(self, correlation_id, timeout=10.0):
        waited = 0.0
        while waited < timeout:
            with self.internal_lock:
                if (
                    correlation_id in self.queue and
                    self.queue[correlation_id] is not None
                ):
                    return self.queue.pop(correlation_id)
            sleep(0.1)
            waited += 0.1
        return None

    def send_request(self, payload, on_response=None):
        corr_id = str(uuid.uuid4())
        body = (
            payload.encode()
            if isinstance(payload, str)
            else payload
        )
        with self.internal_lock:
            self.queue[corr_id] = None
            if on_response:
                self.callbacks[corr_id] = on_response
            self.channel.basic_publish(
                exchange='',
                routing_key=self.rpc_queue,
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=corr_id,
                ),
                body=body
            )
        return corr_id

rpc_client = None

def get_rpc_client():
    global rpc_client
    try:
        if (
            rpc_client is None or
            rpc_client.connection.is_closed
        ):
            rpc_client = RpcClient('rpc_queue')
    except Exception as e:
        print("Reconnecting RPC client:", e)
        rpc_client = RpcClient('rpc_queue')
    return rpc_client


@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    client = get_rpc_client()
    corr_id = client.send_request(payload)
    response = client.wait_for_response(
        corr_id,
        timeout=10.0
    )
    if response is None:
        return 'Timeout waiting for RPC response', 504
    return response

if __name__ == '__main__':
    get_rpc_client()
    app.run(
        host='0.0.0.0',
        port=5000
    )