const ESClient = require('elasticsearch')
const esclient = new ESClient.Client({
    host: 'http://localhost:9200' 
   });

let map1M = new Map();
let map1w = new Map();
let map1d = new Map(); 
let mapSuccessRate = new Map();

let numberOfHits=10000;
let scroll_size_1M=0,sid_1M="",scroll_size_1w=0,sid_1w="",scroll_size_1d=0,sid_1d="";

function counter(map, hits){
    hits.forEach(hit => {
        if((hit._source.status == "Success" || hit._source.status=="Failed" || hit._source.status=="Pending"))
        {
            
            let payment_method = hit._source.context.payment_method;

            if(hit._source.status=="Success"){
                if(map.has(payment_method)==false) map.set(payment_method,[1,1]); // {"pay_method": [total_count,success_count]}
                else map.set(payment_method,[map.get(payment_method)[0]+1,map.get(payment_method)[1]+1]);
                
            }
            else{
                if(map.has(payment_method)==false) map.set(payment_method,[1,0]);
                else map.set(payment_method,[map.get(payment_method)[0]+1,map.get(payment_method)[1]]);
            }
        }
    });
}

esclient.search({
    index: 'ind-qa1-payment-transactions-history-feed',
    scroll:'1m',
    body: {
    from : 0,
        size : numberOfHits,
        query: {
        range:{
            created_date: {
                gte: "now-1M",
                lte: "now-1w"
            }
        }
        }
    }
    }).then(function(res){
    let hits = res.hits.hits;
    console.log(hits.length);
    counter(map1M,hits)

    sid_1M = res._scroll_id;
    scroll_size_1M = res.hits.total;
    scroll_size_1M-=numberOfHits;

    while (scroll_size_1M > 0){
            esclient.scroll({
                body:{
                    scroll: '1m',
                    scroll_id: sid_1M
                }
            }).then(function(resp){
                hits=resp.hits.hits;
                counter(map1M,hits);
            })
            scroll_size_1M-=numberOfHits;   
    }
    console.log(map1M);

    map1M.forEach((Value1M, key) => {                //what if the payment_method is not in last 1M? so for loop on list of paymentmethods
        let successRate1M = Value1M[1]/Value1M[0];
        let total_count = Value1M[1];

        let Value1w = map1w.get(key);
        let successRate1w=0;
        if(Value1w!=null) {
            successRate1w = Value1w[1]/Value1w[0];
            total_count+=Value1w[1];
        }

        let Value1d = map1d.get(key);
        let successRate1d=0;
        if(Value1d!=null) {
            successRate1d = Value1d[1]/Value1d[0];
            total_count+=Value1d[1];
        }
        
        let finalValue = {
            "overallSuccess" :successRate1M,
            "overallSuccessBucket" : (Value1M==null)?"noTransactions":((successRate1M>0.9)?"green":((successRate1M>0.8)?"yellow":"red")),
            "recentSuccess" :successRate1d,
            "recentSuccessBucket" : (Value1d==null)?"noTransactions":((successRate1d>0.9)?"green":((successRate1d>0.8)?"yellow":"red")),   
            "overallFrequency" : total_count,
            "weightedAverage" : (7*successRate1d + 2*successRate1w + 1*successRate1M)/10
        };
        mapSuccessRate.set(key, finalValue);
    })

    console.log(mapSuccessRate);
    }).catch((err) => {
         console.log(err) 
        })

    /////////////////////////// 1week data //////////////////////////
    esclient.search({
        index: 'ind-qa1-payment-transactions-history-feed',
        scroll:'1m',
        body: {
        from : 0,
            size : numberOfHits,
            query: {
            range:{
                created_date: {
                    gte: "now-1w",
                    lte: "now-1d"
                }
            }
            }
        }
        }).then(function(res){
        let hits = res.hits.hits;
        console.log(hits.length);
        counter(map1w,hits);

        sid_1w = res._scroll_id;
        scroll_size_1w = res.hits.total;
    
        scroll_size_1w-=numberOfHits;
    
        while (scroll_size_1w > 0){
                esclient.scroll({
                    body:{
                        scroll: '1m',
                        scroll_id: sid_1w
                    }
                }).then(function(resp){
                    hits=resp.hits.hits;
                    counter(map1w,hits); 
                    })
                scroll_size_1w-=numberOfHits;   
        }
        console.log(map1w);
        }).catch((err) => {
             console.log(err) 
            })

    /////////////////////////// 1day data //////////////////////////

    esclient.search({
    index: 'ind-qa1-payment-transactions-history-feed',
    body: {
    from : 0,
        size : numberOfHits,
        query: {
        range:{
            created_date: {
                gte: "now-1d",
                lte: "now"
            }
        }
        }
    }
    }).then(function(res){
    let hits = res.hits.hits;
    console.log(hits.length);
    counter(map1d,hits);

    sid_1d = res._scroll_id;
    scroll_size_1d = res.hits.total;
    scroll_size_1d-=numberOfHits;

    while (scroll_size_1d > 0){
            esclient.scroll({
                body:{
                    scroll: '1m',
                    scroll_id: sid_1d
                }
            }).then(function(resp){
                hits=resp.hits.hits;
                counter(map1d,hits); 
            })
            scroll_size_1d-=numberOfHits;   
    }
    console.log(map1d);
    })

    module.exports.mapSuccessRate = mapSuccessRate;
    
