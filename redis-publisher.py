# read data from kafka
# send to redis pub channel as hub to reduce pressure of data visit

from kafka import KafkaConsumer

import argparse
import redis
import atexit
import logging

logging.basicConfig()
logger = logging.getLogger('redis-publisher')
logger.setLevel(logging.DEBUG)

def shutdown_hook(kafka_consumer):
	logger.info('shutdown kafka consumer')
	kafka_consumer.close()

if __name__ == '__main__':
	# setup commandline argument
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name', help='kafka topic')
	# localhost:9092
	parser.add_argument('kafka_broker', help='kafka broker location')
	parser.add_argument('redis_host', help='the hostname of redis')
	parser.add_argument('redis_port', help='the port of redis')
	parser.add_argument('redis_channel', help='the channel of redis to publish to')

	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	redis_host = args.redis_host
	redis_port = args.redis_port
	redis_channel = args.redis_channel


	# setup kafka consumer
	kafka_consumer = KafkaConsumer(
		topic_name,
		bootstrap_servers=kafka_broker
	)

	# create a redis client(interact with specific redis cluster)
	redis_client = redis.StrictRedis(host=redis_host, port = redis_port)

	atexit.register(shutdown_hook, kafka_consumer)


	for msg in kafka_consumer:
		logger.info('received data from kafka %s' % str(msg))
		redis_client.publish(redis_channel, msg.value)











