import pika
import os
import requests
import threading
import time
from queue import Queue
from dotenv import load_dotenv

# Load environment variables
def load_env_variables():
    load_dotenv()
    return {
        "amqp_url": os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f'),
        "telegram_token": os.environ.get('telegram_api_key'),
        "telegram_chat_id": os.environ.get('telegram_id'),
        "telegram_channel_id": '-1003097875450'
    }

# Telegram message sender
def send_telegram_message(bot_token, chat_id, message):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        print("[📩] Telegram message sent successfully.")
    else:
        print(f"[⚠️] Failed to send Telegram message. Status code: {response.status_code}")
        print(f"Response: {response.text}")

# Format stock data
def stockdatastore(data):
    return (
        f"Source: {data['source']}\n"
        f"Stock Symbol: {data['symbol']}\n"
        f"Company Name: {data['companyName']}\n"
        f"Volume: {data['volume']}\n"
        f"Last Traded Price: {data['ltp']}\n"
        f"Percentage: {data['pChange']}"
    )

# Rate-limited dispatcher thread
def start_dispatcher(env, message_queue):
    def dispatcher():
        while True:
            if not message_queue.empty():
                message = message_queue.get()
                send_telegram_message(env['telegram_token'], env['telegram_channel_id'], message)
                time.sleep(2)  # 10 messages per second
    threading.Thread(target=dispatcher, daemon=True).start()

# RabbitMQ setup
def create_connection(url):
    params = pika.URLParameters(url)
    return pika.BlockingConnection(params)

def setup_channel(connection, queue_name="hello_world"):
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    return channel

# Message callback
def message_callback(ch, method, properties, body):
    print(f"[✅] Received #{body}")
    env = load_env_variables()
    sample_data = {
        'source': 'NSE',
        'symbol': 'INFY',
        'companyName': 'Infosys Ltd',
        'volume': '1.2M',
        'ltp': '1450.25',
        'pChange': '+2.15%'
    }
    formatted_message = stockdatastore(sample_data)
    message_queue.put(formatted_message)

# Start consumer
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

# Main entry
message_queue = Queue()

def main():
    env = load_env_variables()
    start_dispatcher(env, message_queue)
    connection = create_connection(env['amqp_url'])
    print("[✅] Connection over channel established")
    channel = setup_channel(connection, "etfvolumes")
    start_consumer(channel, "etfvolumes")

if __name__ == "__main__":
    main()
