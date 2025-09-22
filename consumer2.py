import pika
import os
import requests
import time
import json
from dotenv import load_dotenv

def load_env_variables():
    load_dotenv()
    return {
        "amqp_url": os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f'),
        "telegram_token": os.environ.get('telegram_api_key'),
        "telegram_chat_id": os.environ.get('telegram_id'),
        "telegram_channel_id": '-1003097875450'
    }

def create_connection(url):
    params = pika.URLParameters(url)
    return pika.BlockingConnection(params)

def setup_channel(connection, queue_name="hello_world"):
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    return channel

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

def stockdatastore(data, bot_token, channel_id):
    formatted_message = (
        f"Source: {data['source']}\n"
        f"Stock Symbol: {data['symbol']}\n"
        f"Company Name: {data['companyName']}\n"
        f"Volume: {data['volume']}\n"
        f"Last Traded Price: {data['ltp']}\n"
        f"Percentage: {data['pChange']}"
    )
    send_telegram_message(bot_token, channel_id, formatted_message)
    time.sleep(2)

def message_callback(ch, method, properties, body):
    print(f"[✅] Received #{body}")
    # Example usage of stockdatastore with dummy data
    env = load_env_variables()
    sample_data = {
        'source': 'NSE',
        'symbol': 'INFY',
        'companyName': 'Infosys Ltd',
        'volume': '1.2M',
        'ltp': '1450.25',
        'pChange': '+2.15%'
    }
    data = json.loads(body)
    stockdatastore(data, env['telegram_token'], env['telegram_channel_id'])

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
    env = load_env_variables()
    connection = create_connection(env['amqp_url'])
    print("[✅] Connection over channel established")
    channel = setup_channel(connection)
    start_consumer(channel, "etfvolumes")

if __name__ == "__main__":
    main()
