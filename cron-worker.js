const cron = require('node-cron');
let shell = require("shelljs");
const Redis = require('./redis');

const mapSuccessRate = require('./elastic');

cron.schedule("0 */2 * * * *", function(){
    console.log("Cron Started");
    if(shell.exec("node elastic.js").code ===0)
    {
        const map = mapSuccessRate.mapSuccessRate;
         map.forEach( (value, key) => {
            let Value=JSON.stringify(value);
            let returnVal = Redis.SET_ASYNC(key,Value,"EX",3600);
            console.log(JSON.parse(Value));
        })
        
    } else{
        console.log("Something is wrong");
    }
})