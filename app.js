'use strict';

const MongoClient = require('mongodb').MongoClient;
// FOR REAL AWS LAMBDA (NOT LOCAL)
// const AWS = require('aws-sdk');
let atlas_connection_uri;
let cachedDb = null;

exports.handler = (event, context, callback) => {
  const uri = process.env['MONGODB_ATLAS_CLUSTER_URI'];
  // const uri ='mongodb://atlasUser:Intexpmd66693@cluster0-shard-00-00-kwpn3.mongodb.net:27017,cluster0-shard-00-01-kwpn3.mongodb.net:27017,cluster0-shard-00-02-kwpn3.mongodb.net:27017/atlasTest?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true';

  if (atlas_connection_uri != null) {
    processEvent(event, context, callback);
  } else {
    atlas_connection_uri = uri;
    console.log('the Atlas connection string is ' + atlas_connection_uri);
    processEvent(event, context, callback);
    // FOR REAL AWS LAMBDA (NOT LOCAL)
    // const kms = new AWS.KMS();
    //     kms.decrypt({ CiphertextBlob: new Buffer(uri, 'base64') }, (err, data) => {
    //         if (err) {
    //             console.log('Decrypt error:', err);
    //             return callback(err);
    //         }
    //         atlas_connection_uri = data.Plaintext.toString('ascii');
    //         processEvent(event, context, callback);
    //     });
  }
};

const processEvent = async (event, context, callback) => {
  console.log('Calling MongoDB Atlas from AWS Lambda with event: ' + JSON.stringify(event));
  const jsonContents = JSON.parse(JSON.stringify(event));

  //date conversion for grades array
  if (jsonContents.grades != null) {
    for (let i = 0, len = jsonContents.grades.length; i < len; i++) {
      //use the following line if you want to preserve the original dates
      //jsonContents.grades[i].date = new Date(jsonContents.grades[i].date);

      //the following line assigns the current date so we can more easily differentiate between similar records
      jsonContents.grades[i].date = new Date();
    }
  }
  //the following line is critical for performance reasons to allow re-use of database connections across calls to this Lambda function and avoid closing the database connection. The first call to this lambda function takes about 5 seconds to complete, while subsequent, close calls will only take a few hundred milliseconds.
  context.callbackWaitsForEmptyEventLoop = false;

  try {
    if (cachedDb == null) {
      console.log('=> connecting to database');
      const client = await MongoClient.connect(
        atlas_connection_uri,
        { useNewUrlParser: true },
      );
      cachedDb = client.db('atlasTest');
      return createDoc(cachedDb, jsonContents, callback);
    } else {
      createDoc(cachedDb, jsonContents, callback);
    }
  } catch (err) {
    console.error('an error occurred', err);
  }
};

const createDoc = async (db, json, callback) => {
  try {
    const inserted = await db.collection('restaurants').insertOne(json);
    console.log(
      'Kudos! You just created an entry into the restaurants collection with id: ' +
        inserted.insertedId,
    );
    callback(null, 'SUCCESS');
  } catch (err) {
    console.error('an error occurred in createDoc', err);
    callback(null, JSON.stringify(err));
  }
  //we don't need to close the connection thanks to context.callbackWaitsForEmptyEventLoop = false (above)
  //this will let our function re-use the connection on the next called (if it can re-use the same Lambda container)
  //db.close();
};
