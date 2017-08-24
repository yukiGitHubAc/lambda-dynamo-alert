'use strict';

const aws = require('aws-sdk');
const co = require('co');
const dynamodb = new aws.DynamoDB({region: REGION});
const cw = new aws.CloudWatch({region: REGION});
const slack = require('slack');


exports.handler = function (event, context, callback) {
    co(function *() {
        return yield getTableList();
    }).then(targetTableList => {
        targetTableList.forEach((table, count) => {
            getConsumedWriteCapacity(table);
            getConsumedReadCapacity(table);
            if (count === targetTableList.length) callback();
        });
    }).catch(e => {
        callback(e);
    });
};


// ------------------------------------------------------------------------------------
function getTableList(LastEvaluatedTableName, TableList) {
    return new Promise((resolve, reject) => {
        let resList = [];
        let params = {Limit: 100};
        if (LastEvaluatedTableName) params['ExclusiveStartTableName'] = LastEvaluatedTableName;

        dynamodb.listTables(params, function (err, data) {
            if (err) {
                console.log('[ERROR] getTableList function error...', err);
                reject(err);
            } else {
                console.log('[INFO] getTableList...', data);
                if (data.LastEvaluatedTableName) {
                    console.log(data.TableNames);
                    if (!TableList) {
                        TableList = data.TableNames;
                    } else {
                        TableList = TableList.concat(data.TableNames);
                    }
                    getTableList(data.LastEvaluatedTableName, TableList);
                } else {
                    if (TableList) {
                        resList = TableList.concat(data.TableNames);
                    } else {
                        resList = data.TableNames
                    }
                }
                resolve(resList);
            }
        });
    });
}
// ------------------------------------------------------------------------------------
function getConsumedWriteCapacity(tableName) {
    let params = {
        EndTime: new Date(),
        StartTime: new Date(+new Date() - (5 * 60 * 1000)),
        Period: 60,
        Dimensions: [
            {
                Name: 'TableName',
                Value: tableName
            },
        ],
        MetricName: 'ConsumedWriteCapacityUnits',
        Namespace: 'AWS/DynamoDB',
        Statistics: ['Average']
    };
    cw.getMetricStatistics(params, function (err, data) {
        if (err) {
            console.log("Error", err);
        } else {
            if (data.Datapoints.length > 0) {
                let res = data.Datapoints[data.Datapoints.length - 1].Average;
                getProvisionedWriteCapacity(tableName, res);
            } else {
                return null;
            }
        }
    });
}
// ------------------------------------------------------------------------------------
function getConsumedReadCapacity(tableName) {
    let params = {
        EndTime: new Date(),
        StartTime: new Date(+new Date() - (5 * 60 * 1000)),
        Period: 60,
        Dimensions: [
            {
                Name: 'TableName',
                Value: tableName
            },
        ],
        MetricName: 'ConsumedReadCapacityUnits',
        Namespace: 'AWS/DynamoDB',
        Statistics: ['Average']
    };
    cw.getMetricStatistics(params, function (err, data) {
        if (err) {
            console.log("Error", err);
        } else {
            if (data.Datapoints.length > 0) {
                let res = data.Datapoints[data.Datapoints.length - 1].Average;
                getProvisionedReadCapacity(tableName, res);
            } else {
                return null;
            }
        }
    });
}
// ------------------------------------------------------------------------------------
function getProvisionedWriteCapacity(tableName, Capacity) {
    let params = {
        TableName: tableName
    };
    dynamodb.describeTable(params, (err, data) => {
        if (err) {
            console.log("Error", err);
        } else {
            let wcu = data.Table.ProvisionedThroughput.WriteCapacityUnits;
            let writeCapacity = Math.ceil(Number(Capacity));
            console.log('getProvisionedWriteCapacity: ', tableName, writeCapacity, wcu);
            if (writeCapacity > Number(wcu)) {
                console.log('[INFO]', tableName, ': ProvisionedWriteCapacityUnits is ', wcu, 'ConsumedWriteCapacityUnits is ', writeCapacity);
                postSlack(tableName, 'ProvisionedWriteapacityUnits');
                return null;
            } else {
                return null;
            }
        }
    });
}
// ------------------------------------------------------------------------------------
function getProvisionedReadCapacity(tableName, Capacity) {
    let params = {
        TableName: tableName
    };
    dynamodb.describeTable(params, (err, data) => {
        if (err) {
            console.log("Error", err);
        } else {
            let rcu = data.Table.ProvisionedThroughput.ReadCapacityUnits;
            let readCapacity = Math.ceil(Number(Capacity));
            console.log('getProvisionedReadCapacity: ', tableName, readCapacity, rcu);
            if (readCapacity > Number(rcu)) {
                console.log('[INFO]', tableName, ': ProvisionedReadCapacityUnits is ', rcu, 'ConsumedReadCapacityUnits is ', readCapacity);
                postSlack(tableName, 'ProvisionedReadCapacityUnits');
                return null;
            } else {
                return null;
            }
        }
    });
}
// ------------------------------------------------------------------------------------
function postSlack(table, CapacityUnits) {
    let param = {
        token: process.env.token,
        channel: process.env.channel,
        username: process.env.username,
        icon_url: process.env.icon,
        text: "[WARN] Throttling error occurred in DynamoDB. " + "\n" + "Please check　 the　" + table + "(" + CapacityUnits + ") table of DynamoDB."
    };
    slack.chat.postMessage(param, (err, data) => {
        if (err) {
            console.log('[ERROR] slack: ', err);
        }
        else {
            console.log('[INFO] slack', data);
        }
    });
}
