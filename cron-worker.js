const cron = require('node-cron');
let shell = require("shelljs");
const Redis = require('./redis');

const mapSuccessRate = require('./countElastic');

cron.schedule("0 */1 * * * *", function(){
    console.log("Cron Started");
    if(shell.exec("node countElastic.js").code ===0)
    {
        const map = mapSuccessRate.mapSuccessRate;
         map.forEach( (value, key) => {
            let Value=JSON.stringify(value);
            let returnVal = Redis.SET_ASYNC(key,Value,"EX",3600);
            console.log(Value);
        })
        
    } else{
        console.log("Something is wrong");
    }
})