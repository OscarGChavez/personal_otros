const fs = require('fs');
const _AWS = require('aws-sdk');
_AWS.config.loadFromPath('./src/awsconfig.json');
const _s3 = new _AWS.S3();
const _dynamoDb = new _AWS.DynamoDB();
const {DYNAMODB_CONSTANTS, S3_CONSTANTS, MESSAGES, SES_CONSTANTS} = require('./ConstantsAndStrings')

exports.AwsService = {
    getS3Object: async (key, path) => {
        return new Promise((resolve, reject) => {
            let fileStream = fs.createWriteStream(path);
            let s3Stream = _s3.getObject({Bucket: S3_CONSTANTS.BUCKET, Key: key}).createReadStream();
            fileStream.on("error", reject);
            fileStream.on("close", () => { resolve(path);});
            s3Stream.pipe(fileStream);
        });
    },
    newDynamoElement: (tableName, tableColumnsCount) => {
        return { Name: tableName, TableName: tableName, FieldCount: tableColumnsCount, Version: 0, FieldMap: [] };
    },
    getDynamoStructure: (pmresultName) => {
        return new Promise((resolve, reject) => {
            let tableKey = {};
            tableKey[DYNAMODB_CONSTANTS.TABLE_KEY_NAME] = {S: pmresultName};
            _dynamoDb.getItem({Key: tableKey,
                TableName: DYNAMODB_CONSTANTS.TABLE_NAME
            }, (err, result) => {
                if(err) reject(err);
                if(result.hasOwnProperty('Item')){
                    resolve(_AWS.DynamoDB.Converter.unmarshall(result.Item));
                }
                resolve(null);
            });
        });
    },
    setDynamoStructure: (item) => {
        return new Promise((resolve, reject) => {
            _dynamoDb.putItem({Item: _AWS.DynamoDB.Converter.marshall(item),
                TableName: DYNAMODB_CONSTANTS.TABLE_NAME
            }, (err, result) => {
                if(err) reject(err);
                resolve(result);
            });
        });
    },
    sendEmail: (pmresult, notifications) => {
        return new _AWS.SES({apiVersion: '2010-12-01'}).sendEmail(SES_CONSTANTS.PARAMS(pmresult, notifications)).promise();
    },
    copyAndRemoveObject: (s3Key) => {
        return new Promise((resolve, reject) => {
            _s3.copyObject({
                Bucket: S3_CONSTANTS.BUCKET,
                CopySource: S3_CONSTANTS.BUCKET + '/' + s3Key,
                Key: s3Key.replace('processing', 'processed')
            }, function(err, data) {
                if (err) reject(MESSAGES.ERROR.S3_NOT_COPIED(err));
                resolve(data);
            });
        });
    }
};