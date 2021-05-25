const ESClient = require('elasticsearch')
const esclient = new ESClient.Client({
    host: 'http://localhost:9200' 
   });

let mapSuccessRate = new Map();
let payment_method_names = new Map([
["09bfa2be-4329-11ea-b77f-2e728ce88125", "PineLabs Credit Card EMI"],
["1864b0b6-4329-11ea-b77f-2e728ce88125", "PineLabs Debit Card EMI"],
["1d1ce149-3954-4284-bae3-d18c49086f03", "GFESF"],
["2dff6bb4-0e4f-40a2-b161-87866a9a76fb", "PayTM"],
["57cd7bb6-d342-412d-a841-3916ad0e6036", "Netbanking"],
["57d5d10a-4b14-485f-a27e-20966493e15f", "BajajFinserv"],
["5b066725-503e-4545-9907-f042d4020607", "SF"],
["5f615f30-e544-4580-a819-583d5109be34", "MobiKwik"],
["6fd24061-7490-441b-a45a-65cde9fb705e", "qwikcilver"],
["79de4531-bbbc-4336-873c-0eff314b1948", "Debit Card"],
["7be68978-b115-482f-a3d5-984da39c3559", "RTGS/NEFT"],
["7fd6aa07-7f24-4248-8bb7-e714d2b0b688", "OlaMoney"],
["8e42cee4-c4ac-4f6d-a41c-6dc3df529036", "Paytm UPI"],
["8e84d6ea-1ae0-4b39-877e-998620ed639b", "AmazonPay"],
["93463e5f-0e9e-457d-9d30-69e7de478b66", "eNach"],
["b153d87a-e463-4c81-a84e-532048bc87e1", "COD"],
["b153d87a-e463-4c81-a84e-532048bc87e2", "Send Payment Link to Friend"],
["cafa37ab-5752-4218-a271-157d9301441b", "POD"],
["cfb52505-ff01-409b-b429-4515f9ed4f74", "Credit Card"],
["d1d9bd9e-b76e-41ba-b895-d94be177f5fb", "LazyPay"],
["d363a106-d35b-4536-8018-86a0e4970861", "QRSCAN"],
["d463a106-d35b-4536-8018-86a0e4970861", "UPI"],
["d94458b3-c8af-48fd-a413-f3f24c58a9dc", "PhonePe"],
["dce2b46f-85c5-4a89-b032-7ddc385de263", "PineLabs"],
["e8a41d57-af3d-4011-a6f4-570c06ba5c31", "AirtelMoney"],
["ef6d8dee-56a8-4e63-9fe4-bc3e0bb700cc", "Samsung Credit Card"], 
["f7d9febe-8181-426f-9592-33ec5ab7cf4d", "GooglePay"] 
]);

function storage(map,payment_method,status,count){
    if(status=="Success"){
        if(map.has(payment_method)==false) map.set(payment_method,[count,count,0,0]); // {"pay_method": [total_count,success_count,failed_count,pending_count]}
        else map.set(payment_method,[map.get(payment_method)[0]+count,count,map.get(payment_method)[2],map.get(payment_method)[3]]);
    }
    else if(status=="Failed"){
        if(map.has(payment_method)==false) map.set(payment_method,[count,0,count,0]);
        else map.set(payment_method,[map.get(payment_method)[0]+count,map.get(payment_method)[1],map.get(payment_method)[2]+count,map.get(payment_method)[3]]);
    }
    else if(status=="Pending"){
        if(map.has(payment_method)==false) map.set(payment_method,[count,0,0,count]);
        else map.set(payment_method,[map.get(payment_method)[0]+count,map.get(payment_method)[1],map.get(payment_method)[2],map.get(payment_method)[3]+count]);
    }
}

let payment_methods = ["79de4531-bbbc-4336-873c-0eff314b1948","7be68978-b115-482f-a3d5-984da39c3559","d1d9bd9e-b76e-41ba-b895-d94be177f5fb","7fd6aa07-7f24-4248-8bb7-e714d2b0b688","cafa37ab-5752-4218-a271-157d9301441b","f7d9febe-8181-426f-9592-33ec5ab7cf4d","8e42cee4-c4ac-4f6d-a41c-6dc3df529036","cfb52505-ff01-409b-b429-4515f9ed4f74","d463a106-d35b-4536-8018-86a0e4970861","1864b0b6-4329-11ea-b77f-2e728ce88125"
,"57cd7bb6-d342-412d-a841-3916ad0e6036","57d5d10a-4b14-485f-a27e-20966493e15f","b153d87a-e463-4c81-a84e-532048bc87e1","09bfa2be-4329-11ea-b77f-2e728ce88125","8e84d6ea-1ae0-4b39-877e-998620ed639b","5f615f30-e544-4580-a819-583d5109be34","e8a41d57-af3d-4011-a6f4-570c06ba5c31"]

let words="";
function loop() {
    let allPromises=[],Promises=[],response=[];
    for (payment_method of payment_methods) {
      allPromises.push(ElasticQueries(payment_method));
    }
    const promise_all = Promise.all(
        allPromises.map(function(Promises){
            return Promise.all(
                Promises.map(function(response){
                    return Promise.all(response);
                })
            )
        })
    );
    promise_all.then(result =>{
        for(let i=0;i<payment_methods.length;i++){
                
                let one_day = result[i][0][2].count+result[i][1][2].count;
                let one_week = result[i][0][1].count+result[i][1][1].count;
                let one_month = result[i][0][0].count+result[i][1][0].count;

                let total_count = one_month+one_week+one_day+result[i][2][2].count+result[i][2][1].count+result[i][2][0].count;
                let total_success = result[i][0][0].count+result[i][0][1].count+result[i][0][2].count;
                let total_failure = result[i][1][0].count+result[i][1][1].count+result[i][1][2].count;
                let total_pending = result[i][2][0].count+result[i][2][1].count+result[i][2][2].count;
                let successRate1M = (one_month>0)?((result[i][0][0].count)/one_month):0;
                let successRate1w = (one_week>0)?((result[i][0][1].count)/one_week):0;
                let successRate1d = (one_day>0)?((result[i][0][2].count)/one_day):0;
                console.log

                finalValue = {
                    "payment_method_id": payment_method_names.get(payment_methods[i]),
                    "weighted_score" : (7*successRate1d + 2*successRate1w + 1*successRate1M)*10, // ((7x+2y+1z)/10)*100
                    "buckets":[
                        {
                            "interval": "1 day",
                            "total_transactions": one_day+result[i][2][2].count,
                            "total_success": result[i][0][2].count,
                            "total_failure": result[i][1][2].count,
                            "total_pending": result[i][2][2].count
                        },
                        {
                            "interval": "1 week",
                            "total_transactions": one_week+result[i][2][1].count,
                            "total_success": result[i][0][1].count,
                            "total_failure": result[i][1][1].count,
                            "total_pending": result[i][2][1].count
                        },
                        {
                            "interval": "1 month",
                            "total_transactions": one_month+result[i][2][0].count,
                            "total_success": result[i][0][0].count,
                            "total_failure": result[i][1][0].count,
                            "total_pending": result[i][2][0].count
                        }
                    ],
                    "trends":{
                        "plots":{
                            "timestamp":{ 
                                "total_count": total_count,
                                "success_count": total_success,
                                "failure_count": total_failure,
                                "pending_count": total_pending
                            } 
                        }
                    }
                    
                };
                mapSuccessRate.set(payment_methods[i], finalValue); 
        }
        console.log(mapSuccessRate);

    })
}
loop();
//payment_methods.forEach(ElasticQueries);

function ElasticQueries(payment_method){
    Promises=[];   
    let allStatus = ["Success","Failed","Pending"];

    for (status of allStatus) {
       Promises.push(QueryStatus(status));
      }
    return Promises;

    function QueryStatus(status){
        let time_period = ["now-37d","now-15d","now-8d","now-7d"];
        response=[];
        for(let i=0;i<3;i++){
            response[i] = esclient.count({
                index: 'ind-qa1-payments-insights-2021',
                body:{
                    "query": {
                        "bool": {
                        "must": [{
                            "bool":{
                            "should": [
                                {
                                "match": {
                                    "status": status
                                }
                                }
                            ] 
                            }
                            },
                            {
                                "bool":{
                                "should": [
                                    {
                                    "match": {
                                        "context.payment_method": payment_method
                                    }
                                    }
                                ] 
                                }
                            },
                            {
                            "range": {
                                "created_date": {
                                "gte": time_period[i],
                                "lte": time_period[i+1]
                                }
                            }
                            }
                        ]
                        }
                    }
                }
        });
            //  console.log(status+" "+response.count+" "+time_period[i]+" "+payment_method);

            //     if(i==0) storage(map1M,payment_method,status,response.count);
            //     else if(i==1) storage(map1w,payment_method,status,response.count);
            //     else if(i==2) storage(map1d,payment_method,status,response.count);
        }
        return response;
    }
}

module.exports.mapSuccessRate = mapSuccessRate;