const MongoClient = require('mongodb').MongoClient;
//const assert = require('assert');
var http = require('http');
var natural = require('natural'); //for sentiment analysis
var tokenizer = new natural.WordTokenizer(); //break text into arrays of tokens
var Analyzer = natural.SentimentAnalyzer; //// analyze sentiment of the string/text
var stemmer = natural.PorterStemmer;  //reduction of words to their word stem (also known as base or root form) also removes stop words
var analyzer = new Analyzer("English", stemmer,"afinn");  
const createCsvWriter = require("csv-writer").createObjectCsvWriter;  //writing into csv file (export)

// Connection URL
const url = 'mongodb://localhost:27017';

// Database Name
const dbName = "dbtwitter";

// Use connect method to connect to the server
MongoClient.connect(url, {
        useNewUrlParser: true,
        useUnifiedTopology: true
    },
	
    function(err, db) {
        if(err) throw err;
        const dbo = db.db(dbName);

        //create a server object
        http.createServer(function(req, res) 
		{
            //res.setHeader('Content-Type', 'application/json');
			res.setHeader('Content-Type', 'text/html');
            var url = req.url;
			
			var mydq1 = {"user_id" : "923732650627665000"};
			
			var mydq2 = {"user_id" : "18186050"};
			
			var newmydq = {$set : {verified : "TRUE"}};

            if (url == '/insert') 
			{
                //Insert tweet
                var data = {
                    "status_id" : "1350937016365150000", "user_id" : "923732650627665000", "created_at" : "2020-11-05T12:00:00Z","tweet" : "covit sucks", "screen_name" : "covid sucks", "source" : "Twitter for Advertisers", "is_quote" : "FALSE", "is_retweet" : "FALSE", "favourites_count" : "950", "retweet_count" : "10", "followers_count" : "32", "friends_count" : "32", "account_created_at" : "2018-09-29T11:50:15Z", "verified" : "FALSE", "lang" : "en"
                };
				
                dbo.collection("covidinfo1").insertOne(data, function(err, result1) {
                    if (err) {
                      console.log(err)
                    } else {
                    console.log("1 document inserted");
                    res.write("<h3 style='text-align:center;'>Document <mark>inserted</mark></h3>");
                    res.end();
                  }
                    //db.close();
                });
            }
			else if (url == '/delete')
			{
				  //delete tweet for "user_id" : "923732650627665000"
				  dbo.collection("covidinfo1").deleteMany(mydq1, function(err, obj) 
				  {
				  if (err) throw err;
				  console.log("document deleted");
				  res.write("<h3 style='text-align:center;'>Document <mark>deleted</mark></h3>")
				  res.end();
				  
				  });
			}
			else if (url == '/find')
			{
				var resobj = {
					cnt_res : 0,
					result_res : null
				};
				//find "user_id" : "18186050"
				dbo.collection("covidinfo1").find(mydq2).toArray(function(err,result){
                    if (err) {
                      console.log(err)
                    } 
					else {
						console.log("----------------", result);
                        for(var i=0;i<result.length;i++){
                          resobj.cnt_res = resobj.cnt_res + 1
                        }
                        resobj.result_res = result;
						res.write("<h3 style='text-align:center;'><mark>find : Total tweet count and each tweet record information of specific user</mark></h3>")
                        res.end(JSON.stringify(resobj));
					}
                })	
			}
			else if (url == '/update')
			{
				  //update verified info for  "user_id" : "18186050"
				  dbo.collection("covidinfo1").updateMany(mydq2, newmydq, function(err, obj) 
				  {
				  if (err) throw err;
				  console.log("documents updated for given user id");
				  res.write("<h3 style='text-align:center;'>Documents <mark>Updated</mark></h3>")
				  res.end();
				  
				  });
			}
			else if (url == '/aggregate')
			{
				var resobj = {
					cnt_res : 0,
					result_res : null
				};
				//aggregate : result shows count of total number of tweets and total tweets posted by each user having language english
				dbo.collection("covidinfo1").aggregate([{$match : {lang : "en"}}, {$group : { _id : "$user_id", totaltweets : {$sum : 1}}},{ $sort : {totaltweets : -1}}]).toArray(function(err, result) 
				{
                    if (err) {
                      console.log(err)
                    } 
					else {
						console.log("Data aggregation result", result);
						for(var i=0; i<result.length;i++)
						{
							resobj.cnt_res += 1;
						}
						resobj.result_res = result;
						res.write("<h3 style='text-align:center;'>Data <mark>aggregated</mark></h3>");
						res.end(JSON.stringify(resobj));
                  }
                });
				
			}
			else if (url == '/sentiment') 
			{
				/*
				in this method, performing sentiment analysis, for each english tweet in the collection tweet sentiment (i.e tweet is Positive, Negative or Neutral)
				is found and then status_id(i.e tweet id), user_id and sentiment is stored into the sentimentinfo collection.
				*/
				//var cursor = dbo.collection("covidinfo1").find({"lang" : "en"});
					//dbo.collection("covidinfo1").findOne({"status_id" : "1250574628927470000"}, function(err,result){
						
						dbo.listCollections({name : 'sentimentinfo'}).next(function(err,col)
						{
							if(err)
							{
								console.log(err);
							}
							if(col)
							{
								dbo.collection("sentimentinfo").drop(function(err,delok){
									if(err)
									{
										console.log(err);
									}
									if(delok)
									{
										console.log('Collection sentimentinfo dropped as already present!!!');
										dbo.createCollection("sentimentinfo", function(err,result){
											if(err)
											{
												console.log(err);
											}
											console.log("Creating Empty sentimentinfo collection!");
										});
									}
										
								});
							}
						});
						
						dbo.collection("covidinfo1").find({"lang" : "en"},{status_id: 1, user_id : 1, tweet : 1}).toArray(function(err,result){
							console.log('sentiment analysis starts here... wait for few seconds!!!');
							res.write("<h3 style='text-align:center;'><mark>Sentiment Analysis done... Created new collection sentimentinfo</mark></h3>");
							if(err)
							{
								console.log(err);
							}
							result.forEach(function(resu)
							{
									 
									var out = resu.tweet;
									natural.PorterStemmer.attach();
									//console.log(out);
									var tokout = out.tokenizeAndStem();
									//console.log(tokout);
									var sentiment = analyzer.getSentiment(tokout);
									if( sentiment < 0)
									{
										//console.log("Negative");
										//res.write("Negative Sentiment");
										dbo.collection("sentimentinfo").insertOne({status_id : resu.status_id,user_id : resu.user_id,tweet_sentiment : 'Negative' }, function(err, result1) {
											 if (err) {
												console.log(err)
											} else {
												//console.log("1 document inserted");
												res.end();
											 }
										});
									}
									else if ( sentiment > 0)
									{
										//console.log("Positive");
										dbo.collection("sentimentinfo").insertOne({status_id : resu.status_id,user_id : resu.user_id,tweet_sentiment : 'Positive' }, function(err, result1) {
											 if (err) {
												console.log(err)
											} else {
												//console.log("1 document inserted");
												res.end();
											 }
										});										
									}
									else
									{
										//console.log("Neutral");
										dbo.collection("sentimentinfo").insertOne({status_id : resu.status_id,user_id : resu.user_id,tweet_sentiment : 'Neutral' }, function(err, result1) {
										 if (err) {
											console.log(err)
										} else {
											//console.log("1 document inserted");
											res.end();
										 }
										});										
									}
									res.end();
							});
							
					});
			}
			else if (url == '/export') 
			{
				/*in this the sentiment info which we stored into the sentimentinfo collection is get exported into sentimentanalysis.csv excel.
				Excel get created or get overridden(if already present) at the same path from where you are running your application code
				*/
                dbo.collection("sentimentinfo").find({},{_id: 0,"status_id":1,"user_id":1,"tweet_sentiment":1}).toArray(function(err, data) {
                    if (err) {
                      console.log(err)
                    }

					const csvWriter = createCsvWriter({
					  path: "sentimentanalysis.csv",
					  header: [
						{ id: "status_id", title: "status_id" },
						{ id: "user_id", title: "user_id" },
						{ id: "tweet_sentiment", title: "tweet_sentiment" }
					  ]
					});

					csvWriter.writeRecords(data).then(() =>
						console.log("Write to sentimentanalysis.csv file is successful!")
					);
					res.write("<h3 style='text-align:center;'><mark>Write to sentimentanalysis.csv file is successful</mark></h3>");
					res.end();
                });
				
            }
			else if (url == '/smapreduce') 
			{
				/*
				in this method, mapReduce function will get apply to the newly created collection sentimentinfo, 
				which will return the count for each tweet sentiment i.e Positive, Negative or Neutral
				*/
				
                var mapFunction = function() {
					emit(this.tweet_sentiment, 1);
				};

				
				var reduceFunction = function(keySentiment, total) {
				   return Array.sum(total);
				};

				dbo.collection("sentimentinfo").mapReduce(mapFunction,reduceFunction,{ out: {replace : "map_reduce_output"} }, function(err,result){
					if(err)
					{
						console.log(err);
					}

					//find
					dbo.collection("map_reduce_output").find({}).toArray(function(err,result){
						if (err) {
						  console.log(err)
						} 
						else {
							console.log(result);
							res.write("<h3 style='text-align:center;'><mark>MapReduce</mark></h3>");
							res.end(JSON.stringify(result));
						}
					});	
					//res.end();
				});
				
            }
			else if (url == '/join') 
			{			
				/*
				in this method, joining two collections sentimentinfo and covidinfo1 in the form user_id,status_id,tweet_sentiment,retweet_count,
				source and language of Negative tweet (can change here as per requirement)
				*/
				dbo.collection("sentimentinfo").aggregate([{ $lookup : { from : "covidinfo1", localField : "status_id", foreignField : "status_id", as : "tweetinfo"}},{$project : {_id : 0, user_id:1,status_id : 1,tweet_sentiment:1,"tweetinfo.retweet_count":1, "tweetinfo.source":1, "tweetinfo.lang":1}},{$limit : 100},{$match : {"tweet_sentiment" : "Negative"}}]).toArray(function(err, data) {
						if (err) {
						  console.log(err)
						}
						else
						{
							console.log("Join done...");
							console.log(JSON.stringify(data));
							res.write("<h3 style='text-align:center;'><mark>Join Operation on collections covidinfo1 and sentimentinfo</mark></h3>");
							res.end(JSON.stringify(data));
						}
					});
					
				}
				else 
				{
					res.write("<h3 style='text-align:center;'><mark>COVID19 TWITTER DATA ANALYSIS</mark></h3>"); //write a response
					res.end(); //end the response
				}
        }).listen(3000, function() {
            console.log("server start at port 3000"); //the server object listens on port 3000
        });

    });