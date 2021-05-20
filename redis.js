const Redis = require('redis');
const {promisify} = require('util');
const client = Redis.createClient({
    host: '127.0.0.1',
    port: 6379,
})

const GET_ASYNC = promisify(client.get).bind(client);
const SET_ASYNC = promisify(client.set).bind(client);

module.exports.GET_ASYNC = GET_ASYNC;
module.exports.SET_ASYNC = SET_ASYNC;