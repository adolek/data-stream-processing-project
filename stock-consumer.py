from kafka import KafkaConsumer
from datetime import datetime
import json

# Kafka consumer configuration
broker = 'localhost:9092'

consumer = KafkaConsumer(
    bootstrap_servers=[broker],
    group_id='monitor-group'
)

stock_symbols = ['GOOGL', 'META', 'AMZN']

kafka_topics = [f'stock_{stock_symbol}' for stock_symbol in stock_symbols]


def collect_data():

    # Subscribe to all topics
    consumer.subscribe(kafka_topics)

    try:
        while True:
            # Poll for new messages
            records = consumer.poll(timeout_ms=1000)  

            for topic_partition, records in records.items():
                print(f"Topic: {topic_partition.topic}, Partition: {topic_partition.partition}")

                for record in records:
                    # Decode the value from bytes to a string
                    value_str = record.value.decode('utf-8')
                    data_dict = json.loads(value_str)
                    print(data_dict)

            # for topic_partition, messages in records.items():
            #     topic = topic_partition.topic
            #     partition = topic_partition.partition

            #     for message in messages:
            #         # Convert timestamp to UTC datetime
            #         timestamp_ms = message.timestamp
            #         timestamp_dt = datetime.utcfromtimestamp(timestamp_ms / 1000.0)
                    
            #         print(
            #             f"Topic: {topic}, Partition: {partition}, Offset: {message.offset}, Timestamp: {timestamp_dt}"
            #         )

            # consumer.commit()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    collect_data()
