from extract import connect_to_api, extract_json # Add extract_json here
from producer_setup import init_producer, topic
import time

producer = init_producer()

def main():
    response = connect_to_api()

    data = extract_json(response)
    producer = init_producer()
    for stock in data:
        result = {
            'date': stock['date'],
            'symbol': stock['symbol'],
            'open': stock['open'],
            'high': stock['high'],
            'low': stock['low'],
            'close': stock['close'],
            'volume': stock['volume']
        }
        producer.send(topic, result)
        print(f'Data sent to {topic} topic')
        time.sleep(2)     # Sleep for a while to avoid overwhelming the Kafka topic

    producer.flush()  # Ensure all messages are sent before exiting
    producer.close()  # Close the producer connection
    
    print("All stock data successfully delivered and connection closed cleanly.")
    return None

if __name__ == "__main__":
    main()