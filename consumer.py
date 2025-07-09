from kafka import KafkaConsumer
import json
import psycopg2

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    port=5431,                     # Use the new port (5431)
    dbname="bitcoin",
    user="bitcoinuser",
    password="bitcoinpass"
)
cursor = conn.cursor()

# Kafka topics to listen to
TOPICS = [
    'crypto.btc',
    'crypto.eth',
    'crypto.bnb',
    'crypto.xrp',
    'crypto.ada',
    'crypto.doge',
    'crypto.sol',
    'crypto.dot',
    'crypto.ltc',
    'crypto.matic'
]

# Kafka Consumer setup
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='crypto-group'
)

def save_to_postgres(data):
    insert_query = """
        INSERT INTO crypto_prices (timestamp, symbol, usd_price, gbp_price, eur_price, inr_price)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        data.get('timestamp'),
        data.get('symbol'),
        data.get('usd_price', 0),
        data.get('gbp_price', 0),
        data.get('eur_price', 0),
        data.get('inr_price', 0)
    ))
    conn.commit()

print("🚀 Consumer is listening...")

for message in consumer:
    try:
        data = message.value
        print(f"Received: {data}")
        save_to_postgres(data)
    except Exception as e:
        print(f"Error processing message: {e}")
