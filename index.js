'use strict';
const aws = require('aws-sdk');
const co = require('co');
const dynamodb = new aws.DynamoDB({region: REGION});
const cw = new aws.CloudWatch({region: REGION});
const slack = require('slack');

exports.handler = function (event, context, callback) {
    getTableList().then((data) => {
        console.log('[INFO] table list: ', data);
        data.forEach((tableName, count) => {
            let tasks = [
                getReadCapacity(tableName),
                getWriteCapacity(tableName)
            ];
            Promise.all(tasks).then(e => {
                if (e[0] !== undefined) {
                    console.log(e[0]);
                    if (e.warn) {
                        console.log(e[0]);
                        postSlack(e);
                    }
                }
                if (e[1] !== undefined) {
                    console.log(e[1]);
                    if (e.warn) {
                        console.log(e[1]);
                        postSlack(e);
                    }
                }
            }).catch(err => {
                console.log(err)
            });

            if (count === data.length - 1) {
                console.log('table count: ', count + 1);
                callback();
            }
        });
    }).catch(err => {
        console.log(err);
        callback(err);
    });
};


function getTableList(LastEvaluatedTableName, tableList) {
    return new Promise((resolve, reject) => {
        let list = [];
        if (tableList) list = tableList;
        let params = {Limit: 100};
        if (LastEvaluatedTableName) params['ExclusiveStartTableName'] = LastEvaluatedTableName;

        dynamodb.listTables(params, function (err, data) {
            if (err) {
                console.log('[ERROR] getTableList function error...', err);
                reject(err);
            } else {
                if (data.LastEvaluatedTableName) {
                    list = list.concat(data.TableNames);
                    resolve(getTableList(data.LastEvaluatedTableName, list));
                } else {
                    list = list.concat(data.TableNames);
                    resolve(list);
                }
            }
        });
    });
}


function getReadCapacity(tableName) {
    return new Promise((resolve, reject) => {
        let params = {
            EndTime: new Date(),
            StartTime: new Date(+new Date() - (6 * 60 * 1000)),
            Period: 60,
            Dimensions: [
                {
                    Name: 'TableName',
                    Value: tableName
                },
            ],
            MetricName: 'ConsumedReadCapacityUnits',
            Namespace: 'AWS/DynamoDB',
            Statistics: ['Sum']
        };
        cw.getMetricStatistics(params, function (err, data) {
            if (err) {
                console.log("Error", err);
                reject(err);
            } else {
                let sumList = [];
                if (data.Datapoints.length > 0) {
                    data.Datapoints.forEach(data => {
                        let sum = Number(data.Sum) / 60;
                        sumList.push(Number(sum.toFixed(3)));
                    });
                    let val = sumList.reduce(function (previousValue, currentValue) {
                        return previousValue + currentValue;
                    });
                    getProvisionReadCapacity(tableName, Number((val / sumList.length).toFixed(3)), data => {
                        if (data) {
                            resolve(data);
                        } else {
                            resolve(data);
                        }
                    });
                } else {
                    resolve();
                }
            }
        });
    });
}


function getProvisionReadCapacity(tableName, Capacity, callback) {
    let params = {
        TableName: tableName
    };
    dynamodb.describeTable(params, (err, data) => {
        if (err) {
            console.log("Error", err);
        } else {
            let rcu = data.Table.ProvisionedThroughput.ReadCapacityUnits;
            let readCapacity = Math.ceil(Number(Capacity));
            if (readCapacity > Number(rcu)) {
                callback({
                    table: tableName,
                    ProvisionedCapacityUnits: rcu,
                    ConsumedCapacityUnits: Capacity,
                    warn: true,
                    units: 'Read'
                });
            } else {
                callback({
                    table: tableName,
                    ProvisionedCapacityUnits: rcu,
                    ConsumedCapacityUnits: Capacity,
                    warn: false,
                    units: 'Read'
                });
            }
        }
    });
}


function getWriteCapacity(tableName) {
    return new Promise((resolve, reject) => {
        let params = {
            EndTime: new Date(),
            StartTime: new Date(+new Date() - (6 * 60 * 1000)),
            Period: 60,
            Dimensions: [
                {
                    Name: 'TableName',
                    Value: tableName
                },
            ],
            MetricName: 'ConsumedWriteCapacityUnits',
            Namespace: 'AWS/DynamoDB',
            Statistics: ['Sum']
        };
        cw.getMetricStatistics(params, function (err, data) {
            if (err) {
                console.log("Error", err);
                reject(err);
            } else {
                let sumList = [];
                if (data.Datapoints.length > 0) {
                    data.Datapoints.forEach(data => {
                        let sum = Number(data.Sum) / 60;
                        sumList.push(Number(sum.toFixed(3)));
                    });
                    let val = sumList.reduce(function (previousValue, currentValue) {
                        return previousValue + currentValue;
                    });
                    getProvisionWriteCapacity(tableName, Number((val / sumList.length).toFixed(3)), data => {
                        if (data) {
                            resolve(data);
                        } else {
                            resolve(data);
                        }
                    });
                } else {
                    resolve();
                }
            }
        });
    });
}

function getProvisionWriteCapacity(tableName, Capacity, callback) {
    let params = {
        TableName: tableName
    };
    dynamodb.describeTable(params, (err, data) => {
        if (err) {
            console.log("Error", err);
        } else {
            let wcu = data.Table.ProvisionedThroughput.WriteCapacityUnits;
            let writeCapacity = Math.ceil(Number(Capacity));
            if (writeCapacity > Number(wcu)) {
                callback({
                    table: tableName,
                    ProvisionedCapacityUnits: wcu,
                    ConsumedCapacityUnits: Capacity,
                    warn: true,
                    units: 'Write'
                });
            } else {
                callback({
                    table: tableName,
                    ProvisionedCapacityUnits: wcu,
                    ConsumedCapacityUnits: Capacity,
                    warn: false,
                    units: 'Write'
                });
            }
        }
    });
}


function postSlack(data) {
    let param = {
        token: process.env.token,
        channel: process.env.channel,
        username: process.env.username,
        icon_url: process.env.icon,
        text: "[WARN] Throttling error occurred in DynamoDB \n"
        + "Table name: " + data.table + "\n" + "[" + data.units + "] " + data.ProvisionedCapacityUnits + "\n"
        + "INFO" + data
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
// ------------------------------------------------------------------------------------
