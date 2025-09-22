# producer.py

import pika
import os
import json
from dotenv import load_dotenv
from nsepython import nsefetch

def setup_connection():
    load_dotenv()
    url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    print("[✅] Connection over channel established")
    return connection

def setup_channel(connection, queue_name="hello_world"):
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    return channel

def send_to_queue(channel, routing_key, body):
    channel.basic_publish(
        exchange='',
        routing_key=routing_key,
        body=body
    )
    print(f"[📥] Message sent to queue #{body}")

def fetch_nse_volume_gainers():
    url = "https://www.nseindia.com/api/live-analysis-volume-gainers"
    try:
        stocks_data = nsefetch(url)
        return stocks_data.get('data', [])
    except Exception as e:
        print(f"[⚠️] Failed to fetch NSE data: {e}")
        return []

def main():
    connection = setup_connection()
    channel = setup_channel(connection)

#    # Send static messages
#    for _ in range(3):
#        send_to_queue(channel, routing_key="etfvolumes", body="Hello World")

    # Fetch and send NSE stock data
    stock_list = fetch_nse_volume_gainers()
    for stockdata in stock_list:
        payload = {
            "source": "nseindia",
            "symbol": stockdata.get('symbol', 'N/A'),
            "companyName": stockdata.get('companyName', 'N/A'),
            "volume": stockdata.get('volume', 'N/A'),
            "week1AvgVolume": stockdata.get('week1AvgVolume', 'N/A'),
            "week1volChange": stockdata.get('week1volChange', 'N/A'),
            "week2AvgVolume": stockdata.get('week2AvgVolume', 'N/A'),
            "week2volChange": stockdata.get('week2volChange', 'N/A'),
            "ltp": stockdata.get('ltp', 'N/A'),
            "pChange": stockdata.get('pChange', 'N/A')
        }
        send_to_queue(channel, routing_key="etfvolumes", body=json.dumps(payload))

    try:
        connection.close()
        print("[❎] Connection closed")
    except Exception as e:
        print(f"Error: #{e}")

if __name__ == "__main__":
    main()
