import requests
from nsepython import nsefetch
import json

# NSE URL for stock data
url = "https://www.nseindia.com/api/live-analysis-volume-gainers"

# Webhook URL

#webhook_url = "http://localhost:8000/maswebhook"
webhook_url = "https://fastapi-webhook-receiver.vercel.app/maswebhook"


# Fetch stock data
stocks_data = nsefetch(url)
# print(stocks_data)
# Loop through each stock and post to the webhook
for stockdata in stocks_data.get('data', []):
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

    try:
       response = requests.post(webhook_url, json=payload)
       if response.status_code == 200:
           print(f"✅ Sent data for {payload['symbol']}")
       else:
          print(f"⚠️ Failed to send {payload['symbol']}: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        print(f"❌ Error sending data for {payload['symbol']}: {e}")
