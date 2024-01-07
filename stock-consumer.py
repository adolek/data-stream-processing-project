from kafka import KafkaConsumer
from datetime import datetime
import json
import pandas as pd
import statsmodels.api as sm
from river.ensemble import BaggingRegressor
from river.neighbors import KNNRegressor
from river.tree import HoeffdingTreeRegressor
from river.preprocessing import OneHotEncoder, StandardScaler
from river import compose
from sklearn import linear_model, metrics
from evaluate_model import evaluate
import csv

# Define the list of stock symbols and index symbols
stock_symbols = ['GOOGL', 'META', 'AMZN']
index_symbols = ['^GSPC', '^FCHI', '^N225'] # S&P 500, CAC 40, Nikkei 225

# Create Kafka topics for stocks and indices
stock_topics = [f'stock_{stock_symbol}' for stock_symbol in stock_symbols]

# Kafka consumer configuration
broker = 'localhost:9092'
consumer = KafkaConsumer(
    bootstrap_servers=[broker],
    group_id='monitor-group'
)

# Initialize models
rf = BaggingRegressor(HoeffdingTreeRegressor(grace_period=100, leaf_prediction='adaptive', model_selector_decay=0.9), n_models=10)
knn = KNNRegressor(n_neighbors = 10, window_size=100)
ht = HoeffdingTreeRegressor(grace_period=100, leaf_prediction='adaptive', model_selector_decay=0.9)


def kafka_stream_to_csv(file_path):
    # Subscribe to all topics
    consumer.subscribe(stock_topics)

    try:
        with open(file_path, 'w', newline='') as csvfile:
            fieldnames = ['timestamp', 'symbol', 'price', 'sp500', 'cac40', 'nikkei']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            while True:
                # Poll for new messages
                records = consumer.poll(timeout_ms=1000)

                for topic_partition, records in records.items():
                    for record in records:
                        # Decode the value from bytes to a string
                        value_str = record.value.decode('utf-8')
                        data_dict = json.loads(value_str)
                        print(data_dict)
                        
                        # Extract index prices
                        sp500_price = data_dict['index_prices']['^GSPC']
                        cac40_price = data_dict['index_prices']['^FCHI']
                        nikkei_price = data_dict['index_prices']['^N225']

                        # Write data to CSV
                        writer.writerow({
                            'timestamp': data_dict['timestamp'],
                            'symbol': data_dict['symbol'],
                            'price': data_dict['price'],
                            'sp500': sp500_price,
                            'cac40': cac40_price,
                            'nikkei': nikkei_price
                        })

                consumer.commit()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    kafka_stream_to_csv('output_data.csv')
    


# Example of using the streaming data with the evaluate function
# stream = kafka_stream()
# print(strea)
# Adjust parameters as needed
#result_df, dates = evaluate(stream, knn, n_wait=10, verbose=True)





