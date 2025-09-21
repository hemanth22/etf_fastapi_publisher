import requests
from nsepython import nsefetch
import json

# NSE URL for ETF data
url = "https://www.nseindia.com/api/live-analysis-most-active-etf?index=volume"

# Webhook URL
#webhook_url = "https://fastapi-webhook-receiver.vercel.app/webhook"
#webhook_url = "http://localhost:8000/etfwebhook"
webhook_url = "http://localhost:8000/mvetfwebhook"


# Fetch ETF data
etf_data = nsefetch(url)

# Loop through each ETF and post to the webhook
for etf in etf_data.get('data', []):
    payload = {
        "source": "nseindia",
        "symbol": etf.get('symbol', 'N/A'),
        "assetName": etf.get('identifier', 'N/A'),
        "LastPrice": etf.get('lastPrice', 'N/A'),
        "HIGHVALUE": etf.get('dayHigh', 'N/A'),
        "LOWVALUE": etf.get('dayLow', 'N/A'),
        "tradedVolume": etf.get('totalTradedVolume', 'N/A'),
        "tradedValue": etf.get('totalTradedValue', 'N/A'),
        "nav": etf.get('nav', 'N/A'),
        "closePrice": etf.get('closePrice', 'N/A'),
        "PreviousclosePrice": etf.get('previousClose', 'N/A'),
        "PercentageDiff": etf.get('pChange', 'N/A')
    }

    try:
       print(payload)
       response = requests.post(webhook_url, json=payload)
       if response.status_code == 200:
           print(f"✅ Sent data for {payload['symbol']}")
       else:
          print(f"⚠️ Failed to send {payload['symbol']}: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        print(f"❌ Error sending data for {payload['symbol']}: {e}")
