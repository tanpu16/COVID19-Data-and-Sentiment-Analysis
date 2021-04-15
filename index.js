let MongoClient = require('mongodb').MongoClient;
let url = "mongodb://localhost:27017/";
 
const csvFilePath1='CoronavirusTweets1.csv'
const csv1=require('csvtojson')
 
csv1()
.fromFile(csvFilePath1)
.then((jsonObj)=>{
    console.log(jsonObj);
  
  // Insert Json-Object to MongoDB
  MongoClient.connect(url, { useNewUrlParser: true }, (err, db) => {
    if (err) throw err;
    var dbo = db.db("dbtwitter");
    dbo.collection("covidinfo1").insertMany(jsonObj, (err, res) => {
    if (err) throw err;
    console.log("Number of documents inserted: " + res.insertedCount);
    db.close();
    });
  });
})

/*
const csvFilePath2='CoronavirusTweets2.csv'
const csv2=require('csvtojson')
 
csv2()
.fromFile(csvFilePath2)
.then((jsonObj)=>{
    console.log(jsonObj);
  
  // Insert Json-Object to MongoDB
  MongoClient.connect(url, { useNewUrlParser: true }, (err, db) => {
    if (err) throw err;
    var dbo = db.db("dbtwitter");
    dbo.collection("covidinfo2").insertMany(jsonObj, (err, res) => {
    if (err) throw err;
    console.log("Number of documents inserted: " + res.insertedCount);
    db.close();
    });
  });
})
*/