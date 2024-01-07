from kafka import KafkaProducer
import yfinance as yf
import json
from datetime import datetime, timedelta, timezone
import time

# Kafka server config
bootstrap_servers = 'localhost:9092'

# Stocks to analyze
stock_symbols = ['GOOGL', 'META', 'AMZN']
index_symbols = ['^GSPC', '^FCHI', '^N225'] # S&P 500, CAC 40, Nikkei 225
stock_symbols_and_indexes = stock_symbols + index_symbols

# Setup Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to retrieve and publish stock prices
def publish_stock_prices():
    while True:
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')

        # Download data for stocks and indices
        stock_data = yf.download(stock_symbols_and_indexes, start=start_date, end=end_date)

        if not stock_data.empty:
            # Iterate over each timestamp in the stock data
            for index, row in stock_data.iterrows():
                timestamp = index.strftime('%Y-%m-%d %H:%M:%S')

                # Iterate over each stock symbol
                for stock_symbol in stock_symbols:
                    # Extract the stock price and volumes
                    stock_price = float(row['Close'][stock_symbol])

                    # Extract index prices at the given timestamp
                    index_prices = {index_symbol: float(row['Close'][index_symbol]) for index_symbol in index_symbols}

                    # Prepare data for sending
                    data = {
                        'timestamp': timestamp,
                        'symbol': stock_symbol,
                        'price': stock_price,
                        'index_prices': index_prices
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
