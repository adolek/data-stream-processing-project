from kafka import KafkaConsumer
from datetime import datetime
import json
import pandas as pd
import statsmodels.api as sm

# Define the list of stock symbols and index symbols
stock_symbols = ['GOOGL', 'META', 'AMZN']
index_symbols = ['SP500', 'CAC40', 'Nikkei']

# Create Kafka topics for stocks and indices
stock_topics = [f'stock_{stock_symbol}' for stock_symbol in stock_symbols]

# Kafka consumer configuration
broker = 'localhost:9092'
consumer = KafkaConsumer(
    bootstrap_servers=[broker],
    group_id='monitor-group'
)

def online_learning():
    # Subscribe to all topics
    consumer.subscribe(stock_topics)

    # Initialize data storage
    stock_data = {stock_symbol: pd.DataFrame(columns=index_symbols) for stock_symbol in stock_symbols}

    try:
        while True:
            # Poll for new messages
            records = consumer.poll(timeout_ms=1000)  

            for topic_partition, records in records.items():
                for record in records:
                    # Decode the value from bytes to a string
                    value_str = record.value.decode('utf-8')
                    data_dict = json.loads(value_str)

                    symbol = data_dict['symbol']

                    # Update stock data with received feature values
                    if symbol in stock_symbols:
                        stock_data[symbol].loc[data_dict['timestamp']] = data_dict

                        # Perform online learning when new stock data is received
                        stock_name = symbol
                        #y = data_dict['price']  # Dependent variable for the selected stock
                        y = pd.Series(data_dict['price'])  # Convert to pandas Series

                        # Linear Regression
                        X = stock_data[stock_name][index_symbols]  # Independent variables
                        X = sm.add_constant(X)  # Adding a constant

                        lr_model = sm.OLS(y, X).fit()
                        print(f"Linear Regression Model Summary for {stock_name}:\n")
                        print(lr_model.summary())

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    online_learning()
