'''
    Kafka consumer module to read tweets
    fro twitter topic
'''
from kafka import KafkaConsumer

KAFKA_HOST = 'localhost:9092'
TOPIC = 'test'
TIMEOUT = 60000

def main():
    '''
        main module
    '''
    
    # KafkaConsumer Object
    consumer = KafkaConsumer(TOPIC, bootstrap_servers=[KAFKA_HOST],
                             consumer_timeout_ms=TIMEOUT)

    print "TIMEOUT set to:", str(TIMEOUT/1000), "seconds"
    # read messages and get tweet text
    for message in consumer:
        print message.value

if __name__ == '__main__':
    main()