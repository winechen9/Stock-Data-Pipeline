# read from any kafka
# write to any cassandra

import argparse
import logging
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import atexit
import requests

from py_zipkin.zipkin import zipkin_span
from py_zipkin.zipkin import ZipkinAttrs
from py_zipkin.util import generate_random_64bit_string

topic_name = ''
kafka_broker = ''
cassandra_broker = ''
keyspace = ''
table =''

logging.basicConfig()
logger = logging.getLogger('data-storage')
logger.setLevel(logging.DEBUG)

def http_transport_handler(span):
	requests.post('http://localhost:9411/api/v1/spans', data=span, headers={'Content-Type':'application/x-thrift'})

def construct_zipkin_attrs(data):
	parsed = json.loads(data)
	return ZipkinAttrs(
		trace_id = parsed.get('trace_id'),
		parent_span_id = parsed.get('parent_span_id'),
		span_id = generate_random_64bit_string(),
		is_sampled = parsed.get('is_sampled'),
		flags = '0'
		)
		

def save_data(stock_data, session):
	zipkin_attrs = construct_zipkin_attrs(stock_data)
	with zipkin_span(service_name='data-producer', span_name='save_data', transport_handler=http_transport_handler,
		zipkin_attrs = zipkin_attrs):
		try:
			logger.debug('start to save data %s', stock_data)
			parsed = json.loads(stock_data)
			symbol = parsed.get('symbol')
			price = float(parsed.get('price'))
			timestamp =  parsed.get('last_trade_time')

			statement = "INSERT INTO %s (symbol, trade_time, price) VALUES ('%s', '%s', '%f')" % (table, symbol, timestamp, price)
			session.execute(statement)
			logger.info('saved data into cassandra %s', stock_data)

		except Exception as e:
			logger.error('cannot save data %s', stock_data)

def shutdown_hook(consumer, session):
	logger.info('closing resource')
	consumer.close()
	session.shutdown()
	logger.info('released resource')

if __name__ == '__main__':
	# set command line parameters
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name', help = 'the kafka topic name to subscribe from')
	parser.add_argument('kafka_broker', help = 'the kafka broker address')
	parser.add_argument('cassandra_broker', help = 'the cassandra broker location')
	parser.add_argument('keyspace', help= 'the keyspace')
	parser.add_argument('table', help = 'the table in cassandra')

	# parse argument
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	cassandra_broker = args.cassandra_broker
	keyspace = args.keyspace
	table = args.table

	# create kafka consumer 
	consumer = KafkaConsumer(
		topic_name,
		bootstrap_servers = kafka_broker
	)

	# create a cassandra session
	cassandra_cluster = Cluster(
		contact_points = cassandra_broker.split(',')

	)
	session = cassandra_cluster.connect()
	# cql
	session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy','replication_factor': '1'};" % keyspace)
	session.set_keyspace(keyspace)
	session.execute("CREATE TABLE IF NOT EXISTS %s (symbol text, trade_time timestamp, price float, PRIMARY KEY (symbol, trade_time))" % table)

	atexit.register(shutdown_hook, consumer, session)

	for msg in consumer:
		# logger.debug(msg)
		save_data(msg.value, session)

