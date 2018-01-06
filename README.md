## Big Data(Stock Real-Time Price) Pipeline

This is a repo about displaying real-time stock price in a web dashboard with big data frameworks including Kafka, Zookeeper, Cassandra, Spark, Redis, Docker, Zipkin, Node.js, D3.js, Bootstrap and Jquery. 

Python version = Python2.7
Assume the docker-machine's ip is 192.168.99.100

## How to run:
# install all dependencies
pip install -r requirements.txt

# Start the environment
docker run -d -p 2181:2181 -p 2888:2888 -p 3888:3888 --name zookeeper confluent/zookeeper
docker run -d -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME=localhost -e KAFKA_ADVERTISED_PORT=9092 --name kafka --link zookeeper:zookeeper confluent/kafka
docker run -d -p 7199:7199 -p 9042:9042 -p 9160:9160 -p 7001:7001 --name cassandra cassandra:3.7
docker run -d -p 9411:9411 --name zipkin openzipkin/zipkin

# Run data-producer.py
python data-producer.py SYMBOL stock-analyzer 192.168.99.100:9092

# Run data-storage.py
python data-storage.py stock-analyzer 192.168.99.100:9092 192.168.99.100 stock stock

# Run data-stream.py
spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.0.0.jar data-stream.py stock-analyzer 192.168.99.100:9092 average-stock-price

# Run redis-publisher.py
python redis-publisher.py average-stock-price 192.168.99.100:9092 localhost 6379 stock-price

# Run the web app
node public/index.js --redis_host=192.168.99.100 --redis_port=6379 --redis_channel=stock-price

