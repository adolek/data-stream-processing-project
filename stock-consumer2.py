from kafka import KafkaConsumer
from datetime import datetime
import json
import pandas as pd
import statsmodels.api as sm
from sklearn import linear_model, metrics

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

# Define your existing evaluate function
def evaluate(y_true, y_pred, model_name, iteration):
    mae = metrics.mean_absolute_error(y_true, y_pred)
    mse = metrics.mean_squared_error(y_true, y_pred)
    r2 = metrics.r2_score(y_true, y_pred)

    print(f"Iteration {iteration} - Model: {model_name}")
    print(f"MAE: {mae:.4f}")
    print(f"MSE: {mse:.4f}")
    print(f"R^2: {r2:.4f}")
    print("")

def online_learning():
    # Subscribe to all topics
    consumer.subscribe(stock_topics)

    # Initialize data storage
    stock_data = {stock_symbol: pd.DataFrame(columns=index_symbols) for stock_symbol in stock_symbols}

    try:
        iteration = 0
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
                        y_true = data_dict['price']  # Actual value for the selected stock

                        # Linear Regression
                        X = stock_data[stock_name][index_symbols]  # Independent variables
                        X = sm.add_constant(X)  # Adding a constant

                        lr_model = sm.OLS(y_true, X).fit()
                        print(f"Linear Regression Model Summary for {stock_name}:\n")
                        print(lr_model.summary())

                        # Evaluate the model
                        y_pred = lr_model.predict(X)
                        evaluate(y_true, y_pred, stock_name, iteration)
                        iteration += 1

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    online_learning()
