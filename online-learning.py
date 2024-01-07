from kafka import KafkaConsumer
from datetime import datetime
import json
import pandas as pd
import statsmodels.api as sm
import csv
from river.stream import iter_pandas
from river import linear_model


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

# # Define your existing evaluate function
# def evaluate(y_true, y_pred, model_name, iteration):
#     mae = metrics.mean_absolute_error(y_true, y_pred)
#     mse = metrics.mean_squared_error(y_true, y_pred)
#     r2 = metrics.r2_score(y_true, y_pred)

#     print(f"Iteration {iteration} - Model: {model_name}")
#     print(f"MAE: {mae:.4f}")
#     print(f"MSE: {mse:.4f}")
#     print(f"R^2: {r2:.4f}")
#     print("")

def online_learning():
    # Subscribe to all topics
    consumer.subscribe(stock_topics)

    stock_ds = {stock_symbol: {'X': [], 'Y': []} for stock_symbol in stock_symbols}

    # Initialisation du modèle de régression linéaire en ligne
    models = {stock_symbol: linear_model.LinearRegression() for stock_symbol in stock_symbols}

    final_results = {stock_symbol: [] for stock_symbol in stock_symbols}
    results = {stock_symbol: [] for stock_symbol in stock_symbols}

    try:
        #iteration = 0
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
                        stock_ds[symbol]['X'].append(data_dict['index_prices'])
                        stock_ds[symbol]['Y'].append(data_dict['price'])

                        if len(stock_ds[symbol]['X']) % 10 == 0:
                            x = stock_ds[symbol]['X']
                            y = stock_ds[symbol]['Y']
                            
                            results[symbol] = evaluate(stream=iter_pandas(pd.DataFrame(x), pd.Series(y)), model=models[symbol])
                            final_results[symbol].append(results[symbol])
                       

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    online_learning()


