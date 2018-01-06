// get command line argument
var argv = require('minimist')(process.argv.slice(2));
var redis_host = argv['redis_host'];
var redis_port = argv['redis_port'];
var redis_channel = argv['redis_channel'];

var express = require('express');
var app = express();
var server = require('http').createServer(app);


// create socket.io server
var socketio = require('socket.io')(server);


// setup redis client
var redis = require('redis');
console.log('created redis client');
var redisClient = redis.createClient(redis_port, redis_host);
console.log('subscribe to redis topic %s', redis_channel);
redisClient.subscribe(redis_channel);

// register redis callback function
redisClient.on('message', function(channel, message) {
	if (channel == redis_channel) {
		console.log('message received %s', message);
		socketio.sockets.emit('data', message);
	}
});


app.use(express.static(__dirname + '/public'));
app.use('/jquery', express.static(__dirname + '/node_modules/jquery/dist/'));
app.use('/bootstrap', express.static(__dirname + '/node_modules/bootstrap/dist/'));
app.use('/d3', express.static(__dirname + '/node_modules/d3/'))
app.use('/nvd3', express.static(__dirname + '/node_modules/nvd3/build/'));

server.listen(3000, function () {
    console.log('Server started at port 3000.');
});

// - setup shutdown hooks
var shutdown_hook = function () {
    console.log('Quitting redis client');
    redisClient.quit();
    console.log('Shutting down app');
    process.exit();
};

process.on('SIGTERM', shutdown_hook);
process.on('SIGINT', shutdown_hook);
process.on('exit', shutdown_hook);


