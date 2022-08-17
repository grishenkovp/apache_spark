import random
from json import loads, dumps
from kafka import KafkaProducer, KafkaConsumer

def get_random_value():
    current_sale = {}

    list_city = ['city1', 'city2', 'city3', 'city4', 'city5']
    list_manager = ['manager1', 'manager2', 'manager3']
    list_product = ['product1', 'product2', 'product3']

    current_sale['city'] = random.choice(list_city)
    current_sale['manager'] = random.choice(list_manager)
    current_sale['product'] = random.choice(list_product)
    current_sale['amount'] = random.randint(1, 100)

    return current_sale
	
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:dumps(x).encode('utf-8'),
                             compression_type='gzip')
							 							 
test_topic = 'test'
n_records = 7

for _ in range(n_records):
    future = producer.send(topic = test_topic, value = get_random_value())
    record_metadata = future.get(timeout=10)    
    print('--> The message has been sent to a topic: \
            {}, partition: {}, offset: {}' \
            .format(record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset ))
				
#consumer = KafkaConsumer(
#     test_topic,
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='my-group',
#     value_deserializer=lambda x: loads(x.decode('utf-8')))
	 
#for message in consumer:
#    message = message.value
#    print('{}'.format(message))