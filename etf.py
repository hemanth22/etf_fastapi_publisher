import requests
from nsepython import nsefetch
import json

# NSE URL for ETF data
url = "https://www.nseindia.com/api/etf"

# Webhook URL
#webhook_url = "https://fastapi-webhook-receiver.vercel.app/webhook"
webhook_url = "http://localhost:8000/etfwebhook"


# Fetch ETF data
etf_data = nsefetch(url)

# Loop through each ETF and post to the webhook
for etf in etf_data.get('data', []):
    payload = {
        "source": "nseindia",
        "symbol": etf.get('symbol', 'N/A'),
        "assetName": etf.get('assets', 'N/A'),
        "OPENVALUE": etf.get('open', 'N/A'),
        "HIGHVALUE": etf.get('high', 'N/A'),
        "LOWVALUE": etf.get('low', 'N/A'),
        "tradedVolume": etf.get('qty', 'N/A'),
        "tradedValue": etf.get('trdVal', 'N/A'),
        "isin": etf.get('meta', {}).get('isin', 'N/A'),
        "company_name": etf.get('meta', {}).get('companyName', 'N/A'),
        "active_series": etf.get('meta', {}).get('activeSeries', ['N/A'])[0]
    }

    try:
       #print(payload)
       response = requests.post(webhook_url, json=payload)
       if response.status_code == 200:
           print(f"✅ Sent data for {payload['symbol']}")
       else:
          print(f"⚠️ Failed to send {payload['symbol']}: {response.status_code} - {response.text}")
    except requests.RequestException as e:
        print(f"❌ Error sending data for {payload['symbol']}: {e}")
