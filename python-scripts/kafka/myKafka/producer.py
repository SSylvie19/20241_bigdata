import requests
from kafka import KafkaProducer
import json
import logging
import time
from myFaker import generate_random_data

def get_weather():
    data = generate_random_data()
    return data

def main():
    producer = KafkaProducer(bootstrap_servers='kafka:29092')
    while True:
        weather = get_weather()
        weather_send = json.dumps(weather)
        print(weather_send)
        # Chuyển đổi string thành bytes trước khi gửi qua Kafka
        producer.send('weather_data', weather_send.encode('utf-8'))
        #time.sleep(0.1)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()
