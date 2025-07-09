from kafka import KafkaProducer
import json
import time
import requests

# Map symbols to Kafka topics
CRYPTO_TOPICS = {
    'bitcoin': ('crypto.btc', 'BTC'),
    'ethereum': ('crypto.eth', 'ETH'),
    'binancecoin': ('crypto.bnb', 'BNB'),
    'ripple': ('crypto.xrp', 'XRP'),
    'cardano': ('crypto.ada', 'ADA'),
    'dogecoin': ('crypto.doge', 'DOGE'),
    'solana': ('crypto.sol', 'SOL'),
    'polkadot': ('crypto.dot', 'DOT'),
    'litecoin': ('crypto.ltc', 'LTC'),
    'polygon': ('crypto.matic', 'MATIC')
}


# Currency pairs
CURRENCIES = ['usd', 'gbp', 'eur', 'inr']

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_crypto_prices(symbol):
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies={','.join(CURRENCIES)}"
    response = requests.get(url)
    data = response.json()
    
    prices = data.get(symbol, {})
    return prices
    
def produce_data():
    while True:
        for symbol, (topic, ticker) in CRYPTO_TOPICS.items():
            prices = fetch_crypto_prices(symbol)
            if prices:
                message = {
                    'symbol': ticker,
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'usd_price': prices.get('usd', 0),
                    'gbp_price': prices.get('gbp', 0),
                    'eur_price': prices.get('eur', 0),
                    'inr_price': prices.get('inr', 0)
                }
                print(f"Sending to {topic}: {message}")
                producer.send(topic, value=message)
                
            time.sleep(50) # 5 seconds gap between each API call
                
        # Wait for 10 seconds before next fetch
        time.sleep(10)
        
        
if __name__ == '__main__':
    produce_data()