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


def kafka_stream_to_csv(file_path):
    # Subscribe to all topics
    consumer.subscribe(stock_topics)

    try:
        # Create CSV writers for each stock symbol
        csv_writers = {stock_symbol: csv.DictWriter(open(f'dataset_{stock_symbol}.csv', 'w', newline=''),
                                                    fieldnames=['timestamp', 'symbol', 'price', 'sp500', 'cac40', 'nikkei'])
                       for stock_symbol in stock_symbols}
        
        # Write headers to CSV files
        for csv_writer in csv_writers.values():
            csv_writer.writeheader()
        
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

                        # Write data to the corresponding CSV file
                        csv_writer = csv_writers[data_dict['symbol']]
                        csv_writer.writerow({
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





