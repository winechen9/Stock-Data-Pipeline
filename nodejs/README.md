Node.js
index.js
Realize a simple front-end app which displays real-time dynamic data.
Dependencies:
socket.io http://socket.io/

redis https://www.npmjs.com/package/redis

smoothie https://www.npmjs.com/package/smoothie

minimist https://www.npmjs.com/package/minimist

npm install

HOW TO RUN the code:
Assume all your services run on a docker-machine whose ip is 192.168.99.100

node index.js --port=3000 --redis_host=192.168.99.100 --redis_port=6379 --subscribe_topic=average-stock-price