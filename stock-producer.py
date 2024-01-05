from kafka import KafkaProducer
import yfinance as yf
import json
from datetime import datetime, timedelta, timezone
import time

# Kafka server config
bootstrap_servers = 'localhost:9092'

# Stocks to analyze
stock_symbols = ['GOOGL', 'META', 'AMZN']

# stocks = {
#     'America': {
#         'stock': 'GOOGL',
#         'index': 'S&P500',
#     },
#     'Europe': {
#         'stock': 'META',
#         'index': 'CAC40',
#     },
#     'Asia': {
#         'stock': 'AMZN',
#         'index': 'NIKKEI',
#     }
# }

# Setup Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to retrieve and publish stock prices
def publish_stock_prices():
    while True:
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')

        stock_data = yf.download(stock_symbols, start=start_date, end=end_date)
        
        if not stock_data.empty:
            # Iterate over each row in the stock data
            for index, row in stock_data.iterrows():
                timestamp = index.strftime('%Y-%m-%d %H:%M:%S')
                
                # Iterate over each stock symbol
                for stock_symbol in stock_symbols:
                    data = {
                        'timestamp': row.name.strftime('%Y-%m-%d %H:%M:%S'),
                        'symbol': stock_symbol,
                        'price': float(row['Close'][stock_symbol]),
                        'volume': float(row['Volume'][stock_symbol])
                    }
                    # Send the data to the appropriate topic
                    producer.send(f'stock_{stock_symbol}', value=data)
                    print("New data: {}".format(data))
                
        else:
            print("No data retrieved.")
        

try:
    publish_stock_prices()
except KeyboardInterrupt:
    print("Script stop")
