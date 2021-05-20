'use strict';

const Hapi = require('@hapi/hapi');
const ESClient = require('elasticsearch')
const esclient = new ESClient.Client({
    host: 'http://localhost:9200' 
   });

const cron = require('./cron-worker');
const Redis = require('./redis');

const init = async() => {

    const server = Hapi.Server({
        host : 'localhost',
        port : 1234
    });

    server.route({
        method : 'GET',
        path :'/kibana/{payment_method}',
        handler : async (request,response) => {

            const payment_method = request.params.payment_method;
            const reply = await Redis.GET_ASYNC(payment_method);

            if(reply){
                console.log("using cached data");
                return `Rate is (cache hit) ${reply.toString()}`;
            }
            else{
                return 'It is cache miss';
            }
        }
    });

    await server.start();
    console.log(`Server started on : ${server.info.uri}`);
}

process.on('unhandledRejection', (err) => {
    console.log(err);
    process.exit(1);
})
init();


