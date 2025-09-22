import pika
import os
from dotenv import load_dotenv

def load_env_variables():
    load_dotenv()
    return os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')

def create_connection(url):
    params = pika.URLParameters(url)
    return pika.BlockingConnection(params)

def setup_channel(connection, queue_name="hello_world"):
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    return channel

def message_callback(ch, method, properties, body):
    print(f"[✅] Received #{body}")

def start_consumer(channel, queue_name="hello_world"):
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=message_callback,
        auto_ack=True
    )
    try:
        print("\n[❎] Waiting for messages. To exit press CTRL+C \n")
        channel.start_consuming()
    except Exception as e:
        print(f"Error: #{e}")

def main():
    url = load_env_variables()
    connection = create_connection(url)
    print("[✅] Connection over channel established")
    channel = setup_channel(connection)
    start_consumer(channel,"etfvolumes")

if __name__ == "__main__":
    main()
